import os
import html
import time
import sqlite3
import logging
import threading
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, LabeledPrice,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ConversationHandler, PreCheckoutQueryHandler, ContextTypes, Defaults, filters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)  # حتى لا يظهر التوكن في اللوغ
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("shop-bot")

# ───────────────────────── الإعدادات ─────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ.get("ADMIN_CHAT_ID", "0") or 0)

PRODUCT_NAME = os.environ.get("PRODUCT_NAME", "آلة لحام بلاستيك PFS-300")
DEFAULT_DESC = os.environ.get("PRODUCT_DESC", "آلة لحام بلاستيك PFS-300 — جاهزة للاستعمال.")
ENV_PHOTO = os.environ.get("PRODUCT_PHOTO_URL", "")  # اختياري (الأفضل رفع الصورة من البوت)
PRICE_STARS = int(os.environ.get("PRICE_STARS", "5000"))
RESERVE_MINUTES = 10
DB_PATH = os.environ.get("DB_PATH", "shop.db")

PHONE, WILAYA, ADDRESS, CONFIRM = range(4)

WILAYAS = [
    "أدرار", "الشلف", "الأغواط", "أم البواقي", "باتنة", "بجاية", "بسكرة", "بشار", "البليدة", "البويرة",
    "تمنراست", "تبسة", "تلمسان", "تيارت", "تيزي وزو", "الجزائر", "الجلفة", "جيجل", "سطيف", "سعيدة",
    "سكيكدة", "سيدي بلعباس", "عنابة", "قالمة", "قسنطينة", "المدية", "مستغانم", "المسيلة", "معسكر", "ورقلة",
    "وهران", "البيض", "إليزي", "برج بوعريريج", "بومرداس", "الطارف", "تندوف", "تيسمسيلت", "الوادي", "خنشلة",
    "سوق أهراس", "تيبازة", "ميلة", "عين الدفلى", "النعامة", "عين تموشنت", "غرداية", "غليزان", "تيميمون",
    "برج باجي مختار", "أولاد جلال", "بني عباس", "عين صالح", "عين قزام", "تقرت", "جانت", "المغير", "المنيعة",
]

# ───────────────────────── قاعدة البيانات (PostgreSQL أو SQLite تلقائياً) ─────────────────────────
USE_PG = False
pool = None
sqlite_conn = None
sqlite_lock = threading.RLock()


class Cur:
    """غلاف موحّد: يكتب الاستعلامات بـ ? ويحوّلها لـ %s عند PostgreSQL."""

    def __init__(self, cur, pg):
        self.cur, self.pg = cur, pg

    def execute(self, sql, params=()):
        if self.pg:
            sql = sql.replace("?", "%s")
        self.cur.execute(sql, params)

    def fetchone(self):
        r = self.cur.fetchone()
        return dict(r) if r is not None else None

    def fetchall(self):
        return [dict(r) for r in self.cur.fetchall()]


@contextmanager
def cursor():
    if USE_PG:
        conn = pool.getconn()
        try:
            with conn:  # commit / rollback تلقائي
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    yield Cur(c, True)
        finally:
            pool.putconn(conn, close=bool(conn.closed))
    else:
        with sqlite_lock:
            with sqlite_conn:
                c = sqlite_conn.cursor()
                try:
                    yield Cur(c, False)
                finally:
                    c.close()


def init_db():
    global USE_PG, pool, sqlite_conn
    urls = [os.environ.get("DATABASE_URL"), os.environ.get("DATABASE_PUBLIC_URL")]
    for u in [x for x in urls if x]:
        try:
            pool = ThreadedConnectionPool(1, 8, u, connect_timeout=8)
            USE_PG = True
            log.info("Connected to PostgreSQL")
            break
        except Exception as e:
            log.warning("PostgreSQL connection failed: %s", str(e).strip()[:150])

    if not USE_PG:
        sqlite_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        sqlite_conn.row_factory = sqlite3.Row
        log.warning("Using SQLite file: %s", os.path.abspath(DB_PATH))

    pk = "SERIAL PRIMARY KEY" if USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
    with cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shop_product (
                id INT PRIMARY KEY,
                stock INT NOT NULL DEFAULT 1,
                reserved_by BIGINT,
                reserved_until BIGINT
            )
        """)
        cur.execute("INSERT INTO shop_product (id, stock) VALUES (1, 1) ON CONFLICT DO NOTHING")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS shop_orders (
                id {pk},
                user_id BIGINT NOT NULL,
                username TEXT,
                full_name TEXT,
                phone TEXT,
                wilaya TEXT,
                address TEXT,
                amount INT,
                status TEXT DEFAULT 'pending',
                charge_id TEXT UNIQUE,
                created_at BIGINT,
                paid_at BIGINT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shop_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
    # ترحيل: النسخة الأولى أنشأت أعمدة الوقت بنوع TIMESTAMPTZ، والآن نستعمل أرقاماً (epoch)
    if USE_PG:
        with cursor() as cur:
            for tbl, col in (("shop_product", "reserved_until"), ("shop_orders", "created_at"), ("shop_orders", "paid_at")):
                cur.execute(
                    "SELECT data_type FROM information_schema.columns WHERE table_name=? AND column_name=?",
                    (tbl, col),
                )
                r = cur.fetchone()
                if r and str(r["data_type"]).startswith("timestamp"):
                    cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN {col} DROP DEFAULT")
                    cur.execute(
                        f"ALTER TABLE {tbl} ALTER COLUMN {col} TYPE BIGINT USING EXTRACT(EPOCH FROM {col})::BIGINT"
                    )
                    log.info("Migrated %s.%s to BIGINT", tbl, col)
    log.info("DB ready (%s)", "PostgreSQL" if USE_PG else "SQLite")


# ── الإعدادات (صورة المنتج + الوصف) ──
def get_setting(key, default=""):
    with cursor() as cur:
        cur.execute("SELECT value FROM shop_settings WHERE key=?", (key,))
        r = cur.fetchone()
    return r["value"] if r and r["value"] else default


def set_setting(key, value):
    with cursor() as cur:
        if value is None:
            cur.execute("DELETE FROM shop_settings WHERE key=?", (key,))
        else:
            cur.execute("""
                INSERT INTO shop_settings (key, value) VALUES (?, ?)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (key, value))


# ── المخزون والحجز ──
RESERVE_SQL = """
    UPDATE shop_product SET reserved_by = ?, reserved_until = ?
    WHERE id = 1 AND stock > 0
      AND (reserved_by IS NULL OR reserved_until < ? OR reserved_by = ?)
    RETURNING id
"""


def _reserve_params(user_id):
    now = int(time.time())
    return (user_id, now + RESERVE_MINUTES * 60, now, user_id)


def availability(user_id):
    """available / held (محجوزة لزبون آخر) / sold"""
    with cursor() as cur:
        cur.execute("SELECT stock, reserved_by, reserved_until FROM shop_product WHERE id = 1")
        r = cur.fetchone()
    if not r or r["stock"] <= 0:
        return "sold"
    held = (
        r["reserved_by"] is not None
        and (r["reserved_until"] or 0) > int(time.time())
        and r["reserved_by"] != user_id
    )
    return "held" if held else "available"


def reserve(user_id):
    with cursor() as cur:
        cur.execute(RESERVE_SQL, _reserve_params(user_id))
        return cur.fetchone() is not None


def create_order(user, phone, wilaya, address):
    with cursor() as cur:
        cur.execute("UPDATE shop_orders SET status='cancelled' WHERE user_id=? AND status='pending'", (user.id,))
        cur.execute("""
            INSERT INTO shop_orders (user_id, username, full_name, phone, wilaya, address, amount, created_at)
            VALUES (?,?,?,?,?,?,?,?) RETURNING id
        """, (user.id, user.username or "", user.full_name or "", phone, wilaya, address, PRICE_STARS, int(time.time())))
        return cur.fetchone()["id"]


def hold_for_payment(order_id, user_id):
    """يُستدعى في pre_checkout: يتأكد أن الطلب صالح ويمدّد الحجز."""
    with cursor() as cur:
        cur.execute("SELECT status, user_id FROM shop_orders WHERE id=?", (order_id,))
        o = cur.fetchone()
        if not o or o["status"] != "pending" or o["user_id"] != user_id:
            return False
        cur.execute(RESERVE_SQL, _reserve_params(user_id))
        return cur.fetchone() is not None


def mark_paid(order_id, charge_id):
    """returns (status, order) : ok / soldout / dup / invalid"""
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_orders WHERE id=?" + (" FOR UPDATE" if USE_PG else ""), (order_id,))
        o = cur.fetchone()
        if not o:
            return "invalid", None
        if o["status"] in ("paid", "shipped"):
            return "dup", o
        if o["status"] != "pending":
            return "invalid", o
        cur.execute("""
            UPDATE shop_product SET stock = stock - 1, reserved_by = NULL, reserved_until = NULL
            WHERE id = 1 AND stock > 0 RETURNING stock
        """)
        if cur.fetchone() is None:
            return "soldout", o
        cur.execute("""
            UPDATE shop_orders SET status='paid', charge_id=?, paid_at=?
            WHERE id=? RETURNING *
        """, (charge_id, int(time.time()), order_id))
        return "ok", cur.fetchone()


def set_order(order_id, status, charge_id=None):
    with cursor() as cur:
        if charge_id:
            cur.execute("UPDATE shop_orders SET status=?, charge_id=? WHERE id=?", (status, charge_id, order_id))
        else:
            cur.execute("UPDATE shop_orders SET status=? WHERE id=?", (status, order_id))


def get_order(order_id):
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_orders WHERE id=?", (order_id,))
        return cur.fetchone()


def list_orders(limit=10):
    with cursor() as cur:
        cur.execute("""
            SELECT * FROM shop_orders
            WHERE status IN ('paid','shipped','refunded')
            ORDER BY id DESC LIMIT ?
        """, (limit,))
        return cur.fetchall()


def add_stock(n):
    with cursor() as cur:
        cur.execute("UPDATE shop_product SET stock=?, reserved_by=NULL, reserved_until=NULL WHERE id=1", (n,))


def release_stock_one():
    with cursor() as cur:
        cur.execute("UPDATE shop_product SET stock = stock + 1 WHERE id = 1")


# ───────────────────────── نصوص ─────────────────────────
def esc(s):
    return html.escape(str(s or ""))


def order_text(o):
    return (
        f"📦 <b>طلب #{o['id']}</b> — {esc(o['status'])}\n"
        f"👤 {esc(o['full_name'])} (@{esc(o['username'])}) — ID: <code>{o['user_id']}</code>\n"
        f"📱 +{esc(o['phone'])}\n"
        f"📍 {esc(o['wilaya'])}\n"
        f"🏠 {esc(o['address'])}\n"
        f"💰 {o['amount']}⭐"
    )


UNAVAILABLE = {
    "sold": "❌ للأسف، نفدت الكمية. القطعة الوحيدة تم بيعها.",
    "held": "⏳ القطعة محجوزة حالياً لزبون آخر (يدفع الآن). جرّب بعد عدة دقائق، فقد تعود متاحة.",
}


def wilaya_keyboard():
    rows, row = [], []
    for i, name in enumerate(WILAYAS, start=1):
        row.append(InlineKeyboardButton(f"{i:02d} {name}", callback_data=f"w_{i}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("❌ إلغاء", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


# ───────────────────────── /start ─────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    state = availability(uid)
    desc = get_setting("desc", DEFAULT_DESC)
    text = (
        f"🛠 <b>{esc(PRODUCT_NAME)}</b>\n\n"
        f"{esc(desc)}\n\n"
        f"💰 السعر: <b>{PRICE_STARS} ⭐</b> (نجوم تيليجرام)\n"
        f"📦 المتوفر: <b>قطعة واحدة فقط</b>\n"
        f"🇩🇿 البيع والتوصيل داخل الجزائر فقط\n\n"
    )
    markup = None
    if state == "available":
        text += "اضغط الزر أدناه لإتمام الطلب 👇"
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("🛒 اشتري الآن", callback_data="buy")]])
    else:
        text += UNAVAILABLE[state]

    photo = get_setting("photo", ENV_PHOTO)
    if photo:
        try:
            if len(text) <= 1000:
                await update.message.reply_photo(photo, caption=text, reply_markup=markup)
            else:
                await update.message.reply_photo(photo)
                await update.message.reply_text(text, reply_markup=markup)
            return ConversationHandler.END
        except Exception as e:
            log.warning("photo failed: %s", e)
    await update.message.reply_text(text, reply_markup=markup)
    return ConversationHandler.END


# ───────────────────────── خطوات الطلب ─────────────────────────
async def buy_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    state = availability(q.from_user.id)
    if state != "available":
        await q.message.reply_text(UNAVAILABLE[state])
        return ConversationHandler.END
    context.user_data.clear()
    kb = ReplyKeyboardMarkup(
        [[KeyboardButton("📱 مشاركة رقم هاتفي", request_contact=True)]],
        resize_keyboard=True, one_time_keyboard=True,
    )
    await q.message.reply_text(
        "🇩🇿 للتأكد أنك من الجزائر، شارك رقم هاتفك بالضغط على الزر أدناه.\n"
        "(يجب أن يكون الرقم جزائري +213 ومرتبط بحسابك)",
        reply_markup=kb,
    )
    return PHONE


async def got_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    c = update.message.contact
    if c.user_id != update.effective_user.id:
        await update.message.reply_text("⚠️ يرجى مشاركة رقمك أنت، وليس رقم شخص آخر.")
        return PHONE
    phone = "".join(ch for ch in c.phone_number if ch.isdigit())
    if not (phone.startswith("213") and len(phone) == 12):
        await update.message.reply_text(
            "🚫 عذراً، البيع متاح فقط لأصحاب أرقام الهاتف الجزائرية (+213).",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END
    context.user_data["phone"] = phone
    await update.message.reply_text("✅ تم التحقق من رقمك.", reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text("📍 اختر ولايتك:", reply_markup=wilaya_keyboard())
    return WILAYA


async def phone_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("اضغط على زر «📱 مشاركة رقم هاتفي» أسفل الشاشة، أو /cancel للإلغاء.")
    return PHONE


async def got_wilaya(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data.split("_")[1])
    context.user_data["wilaya"] = f"{idx:02d} - {WILAYAS[idx - 1]}"
    await q.message.edit_text(
        f"📍 الولاية: <b>{esc(WILAYAS[idx - 1])}</b>\n\n"
        "🏠 الآن اكتب عنوانك الكامل للتوصيل:\n"
        "البلدية، الحي، الشارع، رقم المنزل، وأي علامة مميزة."
    )
    return ADDRESS


async def got_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    addr = update.message.text.strip()
    if len(addr) < 10 or len(addr) > 300:
        await update.message.reply_text("⚠️ العنوان قصير جداً أو طويل جداً. اكتب عنواناً واضحاً (10 – 300 حرف).")
        return ADDRESS
    context.user_data["address"] = addr
    d = context.user_data
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"💳 تأكيد والدفع {PRICE_STARS}⭐", callback_data="pay")],
        [InlineKeyboardButton("✏️ تعديل الولاية/العنوان", callback_data="restart")],
        [InlineKeyboardButton("❌ إلغاء", callback_data="cancel")],
    ])
    await update.message.reply_text(
        "📋 <b>راجع بياناتك:</b>\n\n"
        f"📱 +{esc(d['phone'])}\n"
        f"📍 {esc(d['wilaya'])}\n"
        f"🏠 {esc(d['address'])}\n\n"
        f"💰 المبلغ: <b>{PRICE_STARS}⭐</b>",
        reply_markup=kb,
    )
    return CONFIRM


async def restart_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.message.reply_text("📍 اختر ولايتك:", reply_markup=wilaya_keyboard())
    return WILAYA


async def pay_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = context.user_data
    uid = q.from_user.id
    if not all(k in d for k in ("phone", "wilaya", "address")):
        await q.message.reply_text("انتهت الجلسة، اكتب /start للبدء من جديد.")
        return ConversationHandler.END
    if not reserve(uid):
        await q.message.reply_text(UNAVAILABLE.get(availability(uid), UNAVAILABLE["sold"]))
        return ConversationHandler.END
    oid = create_order(q.from_user, d["phone"], d["wilaya"], d["address"])
    await q.message.reply_text(f"⏳ القطعة محجوزة لك لمدة {RESERVE_MINUTES} دقائق. أكمل الدفع من الفاتورة أدناه 👇")
    await context.bot.send_invoice(
        chat_id=q.message.chat_id,
        title=PRODUCT_NAME[:32],
        description=f"{PRODUCT_NAME} — توصيل داخل الجزائر"[:255],
        payload=f"order_{oid}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(PRODUCT_NAME[:32], PRICE_STARS)],
    )
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text("تم إلغاء الطلب. اكتب /start للبدء من جديد.", reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text("تم إلغاء الطلب. اكتب /start للبدء من جديد.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


# ───────────────────────── الدفع ─────────────────────────
def parse_order_id(payload):
    try:
        return int(payload.split("_")[1])
    except Exception:
        return None


async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.pre_checkout_query
    oid = parse_order_id(q.invoice_payload)
    ok = (
        oid is not None
        and q.currency == "XTR"
        and q.total_amount == PRICE_STARS
        and hold_for_payment(oid, q.from_user.id)
    )
    if ok:
        await q.answer(ok=True)
    else:
        await q.answer(ok=False, error_message="عذراً، القطعة لم تعد متاحة أو انتهت مهلة الحجز.")


async def notify_admin(context, text):
    if ADMIN_ID:
        try:
            await context.bot.send_message(ADMIN_ID, text)
        except Exception as e:
            log.error("admin notify failed: %s", e)


async def on_paid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sp = update.message.successful_payment
    uid = update.effective_user.id
    oid = parse_order_id(sp.invoice_payload)
    charge = sp.telegram_payment_charge_id
    status, order = mark_paid(oid, charge) if oid else ("invalid", None)

    if status == "ok":
        await update.message.reply_text(
            f"✅ <b>تم الدفع بنجاح!</b>\n\nرقم طلبك: <b>#{oid}</b>\n"
            "سنتواصل معك على رقمك لتأكيد التوصيل قريباً. شكراً لثقتك 🙏"
        )
        await notify_admin(context, "🔔 <b>طلب مدفوع جديد!</b>\n\n" + order_text(order))
    elif status == "dup":
        return
    else:
        # دُفع لكن القطعة لم تعد متاحة → استرجاع تلقائي
        try:
            await context.bot.refund_star_payment(user_id=uid, telegram_payment_charge_id=charge)
            if oid:
                set_order(oid, "refunded", charge)
            await update.message.reply_text("⚠️ نعتذر، القطعة بيعت للتو. تم استرجاع نجومك بالكامل ✅")
            await notify_admin(context, f"⚠️ دفع متأخر للطلب #{oid} (القطعة بيعت) — تم الاسترجاع تلقائياً.")
        except Exception as e:
            log.error("auto refund failed: %s", e)
            await update.message.reply_text("⚠️ حدثت مشكلة، سيتواصل معك المشرف لاسترجاع نجومك.")
            await notify_admin(
                context,
                f"🚨 <b>يلزم استرجاع يدوي</b>\nالمستخدم: <code>{uid}</code>\ncharge_id: <code>{esc(charge)}</code>\nالخطأ: {esc(e)}",
            )


# ───────────────────────── أوامر الأدمن ─────────────────────────
def is_admin(update):
    return bool(update.effective_user) and ADMIN_ID != 0 and update.effective_user.id == ADMIN_ID


async def admin_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """الأدمن يرسل أي صورة للبوت → تصبح صورة المنتج."""
    if not is_admin(update):
        return
    file_id = update.message.photo[-1].file_id
    set_setting("photo", file_id)
    await update.message.reply_text("✅ تم حفظ صورة المنتج. اكتب /start لترى النتيجة.\n(لحذفها: /delphoto)")


async def cmd_delphoto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    set_setting("photo", None)
    await update.message.reply_text("🗑 تم حذف صورة المنتج.")


async def cmd_setdesc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    text = update.message.text.partition(" ")[2].strip()
    if not text:
        await update.message.reply_text("الاستعمال: <code>/setdesc وصف الآلة هنا</code>")
        return
    set_setting("desc", text[:700])
    await update.message.reply_text("✅ تم تحديث وصف المنتج.")


async def cmd_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    orders = list_orders()
    if not orders:
        await update.message.reply_text("لا توجد طلبات مدفوعة بعد.")
        return
    for o in orders:
        await update.message.reply_text(order_text(o))


async def cmd_shipped(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not context.args:
        return
    o = get_order(int(context.args[0]))
    if not o or o["status"] != "paid":
        await update.message.reply_text("الطلب غير موجود أو غير مدفوع.")
        return
    set_order(o["id"], "shipped")
    await update.message.reply_text(f"✅ الطلب #{o['id']} أصبح «تم الشحن».")
    try:
        await context.bot.send_message(o["user_id"], f"🚚 طلبك #{o['id']} في الطريق إليك! سيتصل بك المُوصِّل قريباً.")
    except Exception:
        pass


async def cmd_refund(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not context.args:
        return
    o = get_order(int(context.args[0]))
    if not o or o["status"] not in ("paid", "shipped") or not o["charge_id"]:
        await update.message.reply_text("لا يمكن استرجاع هذا الطلب.")
        return
    try:
        await context.bot.refund_star_payment(user_id=o["user_id"], telegram_payment_charge_id=o["charge_id"])
    except Exception as e:
        await update.message.reply_text(f"فشل الاسترجاع: {esc(e)}")
        return
    if o["status"] == "paid":
        release_stock_one()
    set_order(o["id"], "refunded")
    await update.message.reply_text(f"💸 تم استرجاع {o['amount']}⭐ للطلب #{o['id']}.")
    try:
        await context.bot.send_message(o["user_id"], f"💸 تم استرجاع نجوم الطلب #{o['id']} إلى حسابك.")
    except Exception:
        pass


async def cmd_stock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not context.args:
        return
    add_stock(max(0, int(context.args[0])))
    await update.message.reply_text(f"تم ضبط المخزون على {int(context.args[0])}.")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    await update.message.reply_text(
        "🛠 <b>أوامر الأدمن</b>\n\n"
        "📷 أرسل أي صورة للبوت ← تصبح صورة المنتج\n"
        "/delphoto — حذف الصورة\n"
        "/setdesc نص — تغيير وصف المنتج\n"
        "/orders — الطلبات المدفوعة\n"
        "/shipped رقم — تم الشحن\n"
        "/refund رقم — استرجاع النجوم\n"
        "/stock عدد — ضبط المخزون"
    )


async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled error", exc_info=context.error)


async def post_init(app: Application):
    if not ADMIN_ID:
        return
    msg = "✅ البوت يعمل.\n"
    if USE_PG:
        msg += "🗄 قاعدة البيانات: PostgreSQL"
    else:
        msg += f"🗄 قاعدة البيانات: SQLite ({os.path.abspath(DB_PATH)})"
        if not os.path.abspath(DB_PATH).startswith("/data"):
            msg += (
                "\n\n⚠️ الملف مؤقت: عند إعادة النشر قد يرجع المخزون إلى 1 وتضيع الصورة. "
                "بعد بيع القطعة اكتب <code>/stock 0</code>، أو اربط Volume على /data وضع DB_PATH=/data/shop.db"
            )
    msg += "\n\nاكتب /help لعرض الأوامر."
    try:
        await app.bot.send_message(ADMIN_ID, msg)
    except Exception as e:
        log.warning("startup notice failed: %s", e)


# ───────────────────────── تشغيل ─────────────────────────
def main():
    init_db()
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .defaults(Defaults(parse_mode=ParseMode.HTML))
        .post_init(post_init)
        .build()
    )

    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(buy_cb, pattern="^buy$")],
        states={
            PHONE: [
                MessageHandler(filters.CONTACT, got_phone),
                MessageHandler(filters.TEXT & ~filters.COMMAND, phone_text),
            ],
            WILAYA: [CallbackQueryHandler(got_wilaya, pattern=r"^w_\d+$")],
            ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_address)],
            CONFIRM: [
                CallbackQueryHandler(pay_cb, pattern="^pay$"),
                CallbackQueryHandler(restart_cb, pattern="^restart$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start", start),
            CallbackQueryHandler(cancel, pattern="^cancel$"),
        ],
        allow_reentry=True,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(PreCheckoutQueryHandler(pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, on_paid))
    app.add_handler(MessageHandler(filters.PHOTO, admin_photo))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("delphoto", cmd_delphoto))
    app.add_handler(CommandHandler("setdesc", cmd_setdesc))
    app.add_handler(CommandHandler("orders", cmd_orders))
    app.add_handler(CommandHandler("shipped", cmd_shipped))
    app.add_handler(CommandHandler("refund", cmd_refund))
    app.add_handler(CommandHandler("stock", cmd_stock))
    app.add_error_handler(on_error)

    log.info("Bot started (polling)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
