import os
import re
import html
import time
import sqlite3
import logging
import threading
from contextlib import contextmanager
from contextvars import ContextVar

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, LabeledPrice,
    BotCommand, BotCommandScopeChat, BotCommandScopeDefault, InputMediaPhoto, InputMediaVideo,
    Bot,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ConversationHandler, PreCheckoutQueryHandler, ContextTypes, Defaults, TypeHandler, filters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("shop-bot")

# ───────────────────────── الإعدادات ─────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ.get("ADMIN_CHAT_ID", "0") or 0)

# تُستعمل فقط لإنشاء أول منتج تلقائياً (إذا كانت قاعدة البيانات فارغة)
PRODUCT_NAME = os.environ.get("PRODUCT_NAME", "آلة لحام بلاستيك PFS-300")
DEFAULT_DESC = os.environ.get("PRODUCT_DESC", "آلة لحام بلاستيك PFS-300 — جاهزة للاستعمال.")
ENV_PHOTO = os.environ.get("PRODUCT_PHOTO_URL", "")
PRICE_STARS = int(os.environ.get("PRICE_STARS", "5000"))

RESERVE_MINUTES = 10
MAX_PHOTOS = 10
DB_PATH = os.environ.get("DB_PATH", "shop.db")

# حالات طلب الزبون
PHONE, WILAYA, ADDRESS, CONFIRM = range(4)
# حالات الأدمن
A_NAME, A_PRICE, A_TYPE, A_STOCK, A_DESC, A_PHOTOS, A_DELIV, E_VALUE = range(20, 28)

WILAYAS = [
    "أدرار", "الشلف", "الأغواط", "أم البواقي", "باتنة", "بجاية", "بسكرة", "بشار", "البليدة", "البويرة",
    "تمنراست", "تبسة", "تلمسان", "تيارت", "تيزي وزو", "الجزائر", "الجلفة", "جيجل", "سطيف", "سعيدة",
    "سكيكدة", "سيدي بلعباس", "عنابة", "قالمة", "قسنطينة", "المدية", "مستغانم", "المسيلة", "معسكر", "ورقلة",
    "وهران", "البيض", "إليزي", "برج بوعريريج", "بومرداس", "الطارف", "تندوف", "تيسمسيلت", "الوادي", "خنشلة",
    "سوق أهراس", "تيبازة", "ميلة", "عين الدفلى", "النعامة", "عين تموشنت", "غرداية", "غليزان", "تيميمون",
    "برج باجي مختار", "أولاد جلال", "بني عباس", "عين صالح", "عين قزام", "تقرت", "جانت", "المغير", "المنيعة",
]

# ───────────────────────── سياق المتجر (متعدد البوتات) ─────────────────────────
TOKEN_RE = re.compile(r"^\s*\d{6,12}:[A-Za-z0-9_-]{30,}\s*$")

# المتجر الحالي: id=0 هو البوت الرئيسي، وغيره id = bot_id للبوت الفرعي
_shop = ContextVar("shop", default={"id": 0, "admin": ADMIN_ID, "username": None})


def shop_id():
    return _shop.get()["id"]


def shop_admin():
    return _shop.get()["admin"]


RUNNING = {}  # bot_id -> Application (البوتات الفرعية فقط)

# ───────────────────────── قاعدة البيانات ─────────────────────────
USE_PG = False
pool = None
sqlite_conn = None
sqlite_lock = threading.RLock()


class Cur:
    """غلاف موحّد: الاستعلامات بـ ? وتتحول لـ %s عند PostgreSQL."""

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
            with conn:
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


def ensure_column(table, col, typ):
    if USE_PG:
        with cursor() as cur:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {typ}")
    else:
        with cursor() as cur:
            cur.execute(f"PRAGMA table_info({table})")
            cols = [r["name"] for r in cur.fetchall()]
        if col not in cols:
            with cursor() as cur:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")


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
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS shop_products (
                id {pk},
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                price INT NOT NULL,
                stock INT NOT NULL DEFAULT 1,
                shipping INT NOT NULL DEFAULT 1,
                delivery_text TEXT,
                delivery_file TEXT,
                active INT NOT NULL DEFAULT 1,
                created_at BIGINT
            )
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS shop_photos (
                id {pk},
                product_id INT NOT NULL,
                file_id TEXT NOT NULL,
                kind TEXT DEFAULT 'photo'
            )
        """)
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shop_bots (
                bot_id BIGINT PRIMARY KEY,
                token TEXT NOT NULL,
                username TEXT,
                owner_id BIGINT,
                active INT NOT NULL DEFAULT 1,
                created_at BIGINT
            )
        """)

    # ترحيل أعمدة الطلبات القديمة
    ensure_column("shop_orders", "product_id", "INT")
    ensure_column("shop_orders", "product_name", "TEXT")
    ensure_column("shop_orders", "reserved_until", "BIGINT")
    ensure_column("shop_photos", "kind", "TEXT DEFAULT 'photo'")
    # الصفوف القديمة تأخذ 0 = البوت الرئيسي
    ensure_column("shop_products", "shop_id", "BIGINT DEFAULT 0")
    ensure_column("shop_orders", "shop_id", "BIGINT DEFAULT 0")

    if USE_PG:
        with cursor() as cur:
            for tbl, col in (("shop_orders", "created_at"), ("shop_orders", "paid_at")):
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

    # أول تشغيل: أنشئ منتجاً افتراضياً للبوت الرئيسي (من المنتج القديم إن وُجد)
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM shop_products WHERE shop_id=0")
        n = cur.fetchone()["n"]
    if n == 0:
        stock = 1
        try:
            with cursor() as cur:
                cur.execute("SELECT stock FROM shop_product WHERE id=1")
                r = cur.fetchone()
                if r:
                    stock = r["stock"]
        except Exception:
            pass
        pid = add_product(PRODUCT_NAME, get_setting("desc", DEFAULT_DESC), PRICE_STARS, stock, 1, 1)
        photo = get_setting("photo", ENV_PHOTO)
        if photo:
            add_photo(pid, photo)
        log.info("Created default product #%s", pid)

    log.info("DB ready (%s)", "PostgreSQL" if USE_PG else "SQLite")


# ── الإعدادات القديمة (للترحيل فقط) ──
def get_setting(key, default=""):
    try:
        with cursor() as cur:
            cur.execute("SELECT value FROM shop_settings WHERE key=?", (key,))
            r = cur.fetchone()
        return r["value"] if r and r["value"] else default
    except Exception:
        return default


# ── المنتجات (كلها مقيّدة بالمتجر الحالي) ──
PRODUCT_FIELDS = {"name", "description", "price", "stock", "shipping", "delivery_text", "delivery_file", "active"}


def add_product(name, desc, price, stock, shipping, active=1):
    with cursor() as cur:
        cur.execute("""
            INSERT INTO shop_products (name, description, price, stock, shipping, active, created_at, shop_id)
            VALUES (?,?,?,?,?,?,?,?) RETURNING id
        """, (name, desc or "", price, stock, shipping, active, int(time.time()), shop_id()))
        return cur.fetchone()["id"]


def get_product(pid):
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_products WHERE id=? AND shop_id=?", (pid, shop_id()))
        return cur.fetchone()


def list_products(only_active=True):
    with cursor() as cur:
        if only_active:
            cur.execute("SELECT * FROM shop_products WHERE shop_id=? AND active=1 ORDER BY id", (shop_id(),))
        else:
            cur.execute("SELECT * FROM shop_products WHERE shop_id=? ORDER BY id", (shop_id(),))
        return cur.fetchall()


def update_product(pid, **fields):
    fields = {k: v for k, v in fields.items() if k in PRODUCT_FIELDS}
    if not fields:
        return
    sets = ", ".join(f"{k}=?" for k in fields)
    with cursor() as cur:
        cur.execute(f"UPDATE shop_products SET {sets} WHERE id=? AND shop_id=?", (*fields.values(), pid, shop_id()))


def delete_product(pid):
    with cursor() as cur:
        cur.execute(
            "DELETE FROM shop_photos WHERE product_id IN (SELECT id FROM shop_products WHERE id=? AND shop_id=?)",
            (pid, shop_id()),
        )
        cur.execute("DELETE FROM shop_products WHERE id=? AND shop_id=?", (pid, shop_id()))


def get_photos(pid):
    with cursor() as cur:
        cur.execute("SELECT file_id FROM shop_photos WHERE product_id=? ORDER BY id", (pid,))
        return [r["file_id"] for r in cur.fetchall()]


def get_media(pid):
    """صور وفيديوهات المنتج: [{'file_id':..., 'kind':'photo'|'video'}]"""
    with cursor() as cur:
        cur.execute("SELECT file_id, kind FROM shop_photos WHERE product_id=? ORDER BY id", (pid,))
        return [{"file_id": r["file_id"], "kind": r["kind"] or "photo"} for r in cur.fetchall()]


def add_photo(pid, file_id, kind="photo"):
    with cursor() as cur:
        cur.execute("INSERT INTO shop_photos (product_id, file_id, kind) VALUES (?,?,?)", (pid, file_id, kind))


def clear_photos(pid):
    with cursor() as cur:
        cur.execute("DELETE FROM shop_photos WHERE product_id=?", (pid,))


# ── المخزون والحجز (الحجز = طلبات pending لم تنتهِ مهلتها) ──
def _held_by_others(cur, pid, uid):
    cur.execute("""
        SELECT COUNT(*) AS n FROM shop_orders
        WHERE product_id=? AND status='pending' AND reserved_until > ? AND user_id <> ?
    """, (pid, int(time.time()), uid))
    return cur.fetchone()["n"]


def _lock_product(cur, pid):
    cur.execute("SELECT * FROM shop_products WHERE id=? AND shop_id=?" + (" FOR UPDATE" if USE_PG else ""),
                (pid, shop_id()))
    return cur.fetchone()


def availability(pid, uid):
    """available / held (محجوز لزبون آخر) / sold. المخزون -1 = غير محدود."""
    p = get_product(pid)
    if not p or not p["active"] or p["stock"] == 0:
        return "sold"
    if p["stock"] < 0:
        return "available"
    with cursor() as cur:
        held = _held_by_others(cur, pid, uid)
    return "available" if p["stock"] - held > 0 else "held"


def place_order(user, pid, phone, wilaya, address):
    """ينشئ الطلب ويحجز القطعة. returns (order_id, 'ok') أو (None, 'sold'/'held')"""
    with cursor() as cur:
        p = _lock_product(cur, pid)
        if not p or not p["active"] or p["stock"] == 0:
            return None, "sold"
        if p["stock"] > 0 and p["stock"] - _held_by_others(cur, pid, user.id) <= 0:
            return None, "held"
        cur.execute(
            "UPDATE shop_orders SET status='cancelled' WHERE user_id=? AND product_id=? AND status='pending'",
            (user.id, pid),
        )
        now = int(time.time())
        cur.execute("""
            INSERT INTO shop_orders
              (user_id, username, full_name, phone, wilaya, address, amount, created_at,
               product_id, product_name, reserved_until, shop_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id
        """, (user.id, user.username or "", user.full_name or "", phone, wilaya, address,
              p["price"], now, pid, p["name"], now + RESERVE_MINUTES * 60, shop_id()))
        return cur.fetchone()["id"], "ok"


def hold_for_payment(order_id, user_id):
    """pre_checkout: يتأكد أن الطلب صالح ويمدّد الحجز. يرجع الطلب أو None."""
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_orders WHERE id=? AND shop_id=?", (order_id, shop_id()))
        o = cur.fetchone()
        if not o or o["status"] != "pending" or o["user_id"] != user_id:
            return None
        if o["product_id"]:
            p = _lock_product(cur, o["product_id"])
            if not p or not p["active"] or p["stock"] == 0:
                return None
            if p["stock"] > 0 and p["stock"] - _held_by_others(cur, p["id"], user_id) <= 0:
                return None
        cur.execute(
            "UPDATE shop_orders SET reserved_until=? WHERE id=?",
            (int(time.time()) + RESERVE_MINUTES * 60, order_id),
        )
        return o


def mark_paid(order_id, charge_id):
    """returns (status, order) : ok / soldout / dup / invalid"""
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_orders WHERE id=? AND shop_id=?" + (" FOR UPDATE" if USE_PG else ""),
                    (order_id, shop_id()))
        o = cur.fetchone()
        if not o:
            return "invalid", None
        if o["status"] in ("paid", "shipped"):
            return "dup", o
        if o["status"] != "pending":
            return "invalid", o
        if o["product_id"]:
            p = _lock_product(cur, o["product_id"])
            if not p or p["stock"] == 0:
                return "soldout", o
            if p["stock"] > 0:
                cur.execute(
                    "UPDATE shop_products SET stock = stock - 1 WHERE id=? AND stock > 0 RETURNING stock",
                    (o["product_id"],),
                )
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
            cur.execute("UPDATE shop_orders SET status=?, charge_id=? WHERE id=? AND shop_id=?",
                        (status, charge_id, order_id, shop_id()))
        else:
            cur.execute("UPDATE shop_orders SET status=? WHERE id=? AND shop_id=?",
                        (status, order_id, shop_id()))


def get_order(order_id):
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_orders WHERE id=? AND shop_id=?", (order_id, shop_id()))
        return cur.fetchone()


def list_orders(limit=10):
    with cursor() as cur:
        cur.execute("""
            SELECT * FROM shop_orders
            WHERE shop_id=? AND status IN ('paid','shipped','refunded')
            ORDER BY id DESC LIMIT ?
        """, (shop_id(), limit))
        return cur.fetchall()


def restock_one(pid):
    with cursor() as cur:
        cur.execute("UPDATE shop_products SET stock = stock + 1 WHERE id=? AND shop_id=? AND stock >= 0",
                    (pid, shop_id()))


# ───────────────────────── نصوص ─────────────────────────
def esc(s):
    return html.escape(str(s or ""))


def stock_label(stock):
    return "غير محدود" if stock < 0 else str(stock)


def order_text(o):
    name = o.get("product_name") or PRODUCT_NAME
    uname = f"@{esc(o['username'])}" if o.get("username") else "—"
    t = (
        f"📦 <b>طلب #{o['id']}</b> — {esc(o['status'])}\n"
        f"🛍 {esc(name)}\n"
        f"👤 {esc(o['full_name'])} ({uname}) — ID: <code>{o['user_id']}</code>\n"
    )
    if o.get("wilaya"):
        t += f"📱 +{esc(o['phone'])}\n📍 {esc(o['wilaya'])}\n🏠 {esc(o['address'])}\n"
    else:
        t += "💾 منتج رقمي\n"
    t += f"💰 {o['amount']}⭐"
    return t


UNAVAILABLE = {
    "sold": "❌ للأسف، نفدت الكمية.",
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


# ───────────────────────── واجهة الزبون ─────────────────────────
def is_admin(update):
    a = shop_admin()
    return bool(update.effective_user) and a != 0 and update.effective_user.id == a


async def show_list(message, prods):
    rows = []
    for p in prods:
        mark = "❌ " if p["stock"] == 0 else ("🛠 " if p["shipping"] else "💾 ")
        rows.append([InlineKeyboardButton(f"{mark}{p['name'][:40]} — {p['price']}⭐", callback_data=f"prod_{p['id']}")])
    await message.reply_text("🛍 <b>منتجاتنا</b>\n\nاختر منتجاً لعرض تفاصيله 👇", reply_markup=InlineKeyboardMarkup(rows))


async def show_product(message, p, uid):
    state = availability(p["id"], uid)
    text = f"{'🛠' if p['shipping'] else '💾'} <b>{esc(p['name'])}</b>\n\n"
    if p["description"]:
        text += f"{esc(p['description'])}\n\n"
    text += f"💰 السعر: <b>{p['price']} ⭐</b> (نجوم تيليجرام)\n"
    if p["shipping"]:
        if p["stock"] >= 0:
            text += f"📦 المتوفر: <b>{p['stock']}</b>\n"
        text += "🇩🇿 البيع والتوصيل داخل الجزائر فقط\n"
    else:
        text += "💾 منتج رقمي — يصلك مباشرة بعد الدفع\n"
    text += "\n"

    rows = []
    if state == "available":
        text += "اضغط الزر أدناه لإتمام الطلب 👇"
        rows.append([InlineKeyboardButton("🛒 اشتري الآن", callback_data=f"buy_{p['id']}")])
    else:
        text += UNAVAILABLE[state]
    if len(list_products()) > 1:
        rows.append([InlineKeyboardButton("🔙 كل المنتجات", callback_data="list")])
    markup = InlineKeyboardMarkup(rows) if rows else None

    media = get_media(p["id"])[:MAX_PHOTOS]
    try:
        if len(media) == 1:
            m = media[0]
            send = message.reply_video if m["kind"] == "video" else message.reply_photo
            if len(text) <= 1000:
                await send(m["file_id"], caption=text, reply_markup=markup)
                return
            await send(m["file_id"])
        elif len(media) >= 2:
            await message.reply_media_group([
                InputMediaVideo(m["file_id"]) if m["kind"] == "video" else InputMediaPhoto(m["file_id"])
                for m in media
            ])
    except Exception as e:
        log.warning("media failed: %s", e)
    await message.reply_text(text, reply_markup=markup)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin = is_admin(update)
    if admin:
        await set_admin_commands(context.bot, shop_admin(), shop_id() == 0)
    prods = list_products()
    if not prods:
        msg = "🚧 لا توجد منتجات حالياً."
        if admin:
            msg += "\n\nأنت الأدمن: اكتب /addproduct لإضافة أول منتج."
        await update.message.reply_text(msg)
        return ConversationHandler.END
    if len(prods) == 1:
        await show_product(update.message, prods[0], update.effective_user.id)
    else:
        await show_list(update.message, prods)
    return ConversationHandler.END


async def prod_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    p = get_product(int(q.data.split("_")[1]))
    if not p or not p["active"]:
        await q.message.reply_text("هذا المنتج لم يعد متوفراً.")
        return
    await show_product(q.message, p, q.from_user.id)


async def list_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    prods = list_products()
    if prods:
        await show_list(q.message, prods)


# ───────────────────────── خطوات الطلب ─────────────────────────
async def send_summary(message, context):
    d = context.user_data
    p = get_product(d["pid"])
    rows = [[InlineKeyboardButton(f"💳 تأكيد والدفع {p['price']}⭐", callback_data="pay")]]
    if p["shipping"]:
        rows.append([InlineKeyboardButton("✏️ تعديل الولاية/العنوان", callback_data="restart")])
    rows.append([InlineKeyboardButton("❌ إلغاء", callback_data="cancel")])
    text = f"📋 <b>راجع طلبك:</b>\n\n🛍 {esc(p['name'])}\n"
    if p["shipping"]:
        text += f"📱 +{esc(d['phone'])}\n📍 {esc(d['wilaya'])}\n🏠 {esc(d['address'])}\n"
    else:
        text += "💾 منتج رقمي — يصلك مباشرة بعد الدفع\n"
    text += f"\n💰 المبلغ: <b>{p['price']}⭐</b>"
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(rows))


async def buy_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pid = int(q.data.split("_")[1])
    state = availability(pid, q.from_user.id)
    if state != "available":
        await q.message.reply_text(UNAVAILABLE[state])
        return ConversationHandler.END
    p = get_product(pid)
    context.user_data.clear()
    context.user_data["pid"] = pid

    if not p["shipping"]:  # منتج رقمي: لا هاتف ولا ولاية ولا عنوان
        await send_summary(q.message, context)
        return CONFIRM

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
    await send_summary(update.message, context)
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
    pid = d.get("pid")
    p = get_product(pid) if pid else None
    if not p:
        await q.message.reply_text("انتهت الجلسة، اكتب /start للبدء من جديد.")
        return ConversationHandler.END
    if p["shipping"] and not all(k in d for k in ("phone", "wilaya", "address")):
        await q.message.reply_text("انتهت الجلسة، اكتب /start للبدء من جديد.")
        return ConversationHandler.END

    oid, why = place_order(q.from_user, pid, d.get("phone"), d.get("wilaya"), d.get("address"))
    if not oid:
        await q.message.reply_text(UNAVAILABLE.get(why, UNAVAILABLE["sold"]))
        return ConversationHandler.END
    order = get_order(oid)

    await q.message.reply_text(f"⏳ تم حجز طلبك لمدة {RESERVE_MINUTES} دقائق. أكمل الدفع من الفاتورة أدناه 👇")
    desc = (p["description"] or p["name"])
    if p["shipping"]:
        desc = f"{p['name']} — توصيل داخل الجزائر"
    await context.bot.send_invoice(
        chat_id=q.message.chat_id,
        title=p["name"][:32],
        description=desc[:255],
        payload=f"order_{oid}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(p["name"][:32], order["amount"])],
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
    o = hold_for_payment(oid, q.from_user.id) if (oid is not None and q.currency == "XTR") else None
    if o is not None and q.total_amount == o["amount"]:
        await q.answer(ok=True)
    else:
        await q.answer(ok=False, error_message="عذراً، المنتج لم يعد متاحاً أو انتهت مهلة الحجز.")


async def notify_admin(context, text):
    a = shop_admin()
    if a:
        try:
            await context.bot.send_message(a, text)
        except Exception as e:
            log.error("admin notify failed: %s", e)


async def deliver_digital(context, chat_id, p):
    """يرسل المحتوى الرقمي للزبون. يرجع True إذا أُرسل شيء."""
    sent = False
    if p.get("delivery_text"):
        await context.bot.send_message(chat_id, f"🎁 <b>منتجك:</b>\n\n{esc(p['delivery_text'])}")
        sent = True
    if p.get("delivery_file"):
        await context.bot.send_document(chat_id, p["delivery_file"])
        sent = True
    return sent


async def on_paid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sp = update.message.successful_payment
    uid = update.effective_user.id
    oid = parse_order_id(sp.invoice_payload)
    charge = sp.telegram_payment_charge_id
    status, order = mark_paid(oid, charge) if oid else ("invalid", None)

    if status == "ok":
        p = get_product(order["product_id"]) if order.get("product_id") else None
        if p and not p["shipping"]:
            await update.message.reply_text(f"✅ <b>تم الدفع بنجاح!</b>\n\nرقم طلبك: <b>#{oid}</b>")
            ok = False
            try:
                ok = await deliver_digital(context, update.effective_chat.id, p)
            except Exception as e:
                log.error("digital delivery failed: %s", e)
            if not ok:
                await update.message.reply_text("سيصلك المنتج من المشرف قريباً 🙏")
                await notify_admin(context, f"⚠️ طلب رقمي #{oid} بلا محتوى تسليم! أرسله للزبون يدوياً (ID: <code>{uid}</code>).")
        else:
            await update.message.reply_text(
                f"✅ <b>تم الدفع بنجاح!</b>\n\nرقم طلبك: <b>#{oid}</b>\n"
                "سنتواصل معك على رقمك لتأكيد التوصيل قريباً. شكراً لثقتك 🙏"
            )
        await notify_admin(context, "🔔 <b>طلب مدفوع جديد!</b>\n\n" + order_text(order))
    elif status == "dup":
        return
    else:
        try:
            await context.bot.refund_star_payment(user_id=uid, telegram_payment_charge_id=charge)
            if oid:
                set_order(oid, "refunded", charge)
            await update.message.reply_text("⚠️ نعتذر، المنتج بيع للتو. تم استرجاع نجومك بالكامل ✅")
            await notify_admin(context, f"⚠️ دفع متأخر للطلب #{oid} (نفدت الكمية) — تم الاسترجاع تلقائياً.")
        except Exception as e:
            log.error("auto refund failed: %s", e)
            await update.message.reply_text("⚠️ حدثت مشكلة، سيتواصل معك المشرف لاسترجاع نجومك.")
            await notify_admin(
                context,
                f"🚨 <b>يلزم استرجاع يدوي</b>\nالمستخدم: <code>{uid}</code>\ncharge_id: <code>{esc(charge)}</code>\nالخطأ: {esc(e)}",
            )


# ───────────────────────── لوحة الأدمن: المنتجات ─────────────────────────
def adm(context):
    return context.user_data.setdefault("adm", {})


def panel_text(p):
    n = len(get_photos(p["id"]))
    t = (
        f"🗂 <b>{esc(p['name'])}</b>  (#{p['id']})\n\n"
        f"النوع: {'🚚 مادي (شحن + ولايات)' if p['shipping'] else '💾 رقمي (بدون شحن)'}\n"
        f"💰 السعر: {p['price']}⭐\n"
        f"📦 المخزون: {stock_label(p['stock'])}\n"
        f"📷 الوسائط (صور/فيديو): {n}\n"
        f"الحالة: {'✅ ظاهر للزبائن' if p['active'] else '🚫 مخفي'}\n"
    )
    if not p["shipping"]:
        t += (
            f"🎁 محتوى التسليم: نص {'✅' if p['delivery_text'] else '❌'} | "
            f"ملف {'✅' if p['delivery_file'] else '❌'}\n"
        )
    if p["description"]:
        t += f"\n📝 {esc(p['description'][:200])}"
    return t


def panel_markup(p):
    i = p["id"]
    rows = [
        [InlineKeyboardButton("✏️ الاسم", callback_data=f"edt_name_{i}"),
         InlineKeyboardButton("📝 الوصف", callback_data=f"edt_desc_{i}")],
        [InlineKeyboardButton("💰 السعر", callback_data=f"edt_price_{i}"),
         InlineKeyboardButton("📊 المخزون", callback_data=f"edt_stock_{i}")],
        [InlineKeyboardButton("📷 تغيير الصور/الفيديو", callback_data=f"edt_photos_{i}"),
         InlineKeyboardButton("🧹 مسح الوسائط", callback_data=f"adm_clrphotos_{i}")],
        [InlineKeyboardButton("🔁 تحويل إلى رقمي" if p["shipping"] else "🔁 تحويل إلى مادي (شحن)",
                              callback_data=f"adm_ship_{i}")],
    ]
    if not p["shipping"]:
        rows.append([InlineKeyboardButton("🎁 محتوى التسليم", callback_data=f"edt_deliv_{i}")])
    rows.append([InlineKeyboardButton("🚫 إخفاء" if p["active"] else "✅ إظهار", callback_data=f"adm_active_{i}"),
                 InlineKeyboardButton("❌ حذف", callback_data=f"adm_del_{i}")])
    rows.append([InlineKeyboardButton("🔙 المنتجات", callback_data="adm_list")])
    return InlineKeyboardMarkup(rows)


async def render_panel(message, pid, edit=False):
    p = get_product(pid)
    if not p:
        await message.reply_text("المنتج غير موجود.")
        return
    if edit:
        try:
            await message.edit_text(panel_text(p), reply_markup=panel_markup(p))
            return
        except Exception:
            pass
    await message.reply_text(panel_text(p), reply_markup=panel_markup(p))


async def send_products_admin(message):
    prods = list_products(only_active=False)
    rows = [[InlineKeyboardButton(
        f"{'✅' if p['active'] else '🚫'} {'🛠' if p['shipping'] else '💾'} {p['name'][:30]} — {p['price']}⭐",
        callback_data=f"adm_panel_{p['id']}")] for p in prods]
    rows.append([InlineKeyboardButton("➕ إضافة منتج", callback_data="adm_new")])
    await message.reply_text("🗂 <b>المنتجات</b>\nاختر منتجاً لإدارته:", reply_markup=InlineKeyboardMarkup(rows))


async def cmd_products(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    await send_products_admin(update.message)


async def adm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أزرار الأدمن خارج المحادثات (فتح اللوحة، تبديل، حذف...)."""
    q = update.callback_query
    if not is_admin(update):
        await q.answer()
        return
    await q.answer()
    parts = q.data.split("_")  # adm_<action>_<pid>
    action, pid = parts[1], int(parts[2])
    p = get_product(pid)
    if not p:
        await q.message.reply_text("المنتج غير موجود.")
        return

    if action == "panel":
        await render_panel(q.message, pid)
    elif action == "ship":
        new = 0 if p["shipping"] else 1
        stock = p["stock"]
        if new == 0 and stock == 0:
            stock = -1          # رقمي: غير محدود
        if new == 1 and stock < 0:
            stock = 1           # مادي: ضع كمية
        update_product(pid, shipping=new, stock=stock)
        await render_panel(q.message, pid, edit=True)
    elif action == "active":
        update_product(pid, active=0 if p["active"] else 1)
        await render_panel(q.message, pid, edit=True)
    elif action == "clrphotos":
        clear_photos(pid)
        await render_panel(q.message, pid, edit=True)
    elif action == "del":
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ نعم، احذف", callback_data=f"adm_delyes_{pid}"),
            InlineKeyboardButton("🔙 لا", callback_data=f"adm_panel_{pid}"),
        ]])
        await q.message.reply_text(f"⚠️ حذف «{esc(p['name'])}» نهائياً؟", reply_markup=kb)
    elif action == "delyes":
        delete_product(pid)
        await q.message.edit_text("🗑 تم حذف المنتج.")


async def adm_list_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if is_admin(update):
        await send_products_admin(q.message)


# ── إضافة منتج (محادثة) ──
async def begin_new(message, context):
    context.user_data["adm"] = {"mode": "new"}
    await message.reply_text("➕ <b>منتج جديد</b>\n\nاكتب <b>اسم المنتج</b>:\n(للإلغاء: /cancel)")
    return A_NAME


async def cmd_addproduct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ConversationHandler.END
    return await begin_new(update.message, context)


async def new_product_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        return ConversationHandler.END
    return await begin_new(q.message, context)


async def got_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if len(name) < 2 or len(name) > 100:
        await update.message.reply_text("⚠️ اكتب اسماً من 2 إلى 100 حرف.")
        return A_NAME
    adm(context)["name"] = name
    await update.message.reply_text("💰 اكتب <b>السعر</b> بالنجوم ⭐ (رقم فقط):")
    return A_PRICE


async def got_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if not t.isdigit() or not (1 <= int(t) <= 1000000):
        await update.message.reply_text("⚠️ اكتب رقماً صحيحاً أكبر من 0.")
        return A_PRICE
    adm(context)["price"] = int(t)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚚 مادي — مع شحن (ولايات + عنوان)", callback_data="nt_ship")],
        [InlineKeyboardButton("💾 رقمي — بدون شحن", callback_data="nt_dig")],
    ])
    await update.message.reply_text("اختر <b>نوع المنتج</b>:", reply_markup=kb)
    return A_TYPE


async def got_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ship = 1 if q.data == "nt_ship" else 0
    adm(context)["shipping"] = ship
    if ship:
        await q.message.reply_text("📦 اكتب <b>الكمية المتوفرة</b> (رقم):")
    else:
        await q.message.reply_text("📦 اكتب عدد النسخ المتاحة، أو <b>0</b> لعدد غير محدود:")
    return A_STOCK


def parse_stock(text, shipping):
    t = text.strip()
    if not t.isdigit() or int(t) > 1000000:
        return None
    n = int(t)
    if n == 0 and not shipping:
        return -1
    return n


async def got_stock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    n = parse_stock(update.message.text, adm(context)["shipping"])
    if n is None:
        await update.message.reply_text("⚠️ اكتب رقماً صحيحاً.")
        return A_STOCK
    adm(context)["stock"] = n
    await update.message.reply_text("📝 اكتب <b>وصف المنتج</b> (أو /skip للتخطي):")
    return A_DESC


async def create_draft(update, context, desc):
    a = adm(context)
    a["pid"] = add_product(a["name"], desc, a["price"], a["stock"], a["shipping"], active=0)
    await update.message.reply_text(
        f"📷 أرسل <b>صور و/أو فيديوهات المنتج</b> (حتى {MAX_PHOTOS} في المجموع، كصور/فيديو وليس كملفات).\n"
        "عند الانتهاء اكتب /done — أو /skip إذا لا تريد وسائط."
    )
    return A_PHOTOS


async def got_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await create_draft(update, context, update.message.text.strip()[:700])


async def skip_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await create_draft(update, context, "")


async def got_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    pid = a["pid"]
    if a.pop("replace", False):
        clear_photos(pid)
    n = len(get_photos(pid))
    if n >= MAX_PHOTOS:
        await update.message.reply_text(f"⚠️ الحد الأقصى {MAX_PHOTOS} صور. اكتب /done للمتابعة.")
        return A_PHOTOS
    if update.message.video:
        add_photo(pid, update.message.video.file_id, "video")
        what = "الفيديو"
    else:
        add_photo(pid, update.message.photo[-1].file_id, "photo")
        what = "الصورة"
    await update.message.reply_text(f"✅ تمت إضافة {what} ({n + 1}/{MAX_PHOTOS}). أرسل غيرها أو اكتب /done.")
    return A_PHOTOS


async def photos_wrong(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚠️ أرسل صورة أو فيديو (وليس كملف)، أو /done للمتابعة.")
    return A_PHOTOS


async def finish_admin(update, context):
    a = adm(context)
    pid = a.get("pid")
    if a.get("mode") == "new" and pid:
        update_product(pid, active=1)
        await update.effective_message.reply_text("✅ تم حفظ المنتج ونشره.")
    context.user_data.pop("adm", None)
    if pid:
        await render_panel(update.effective_message, pid)
    return ConversationHandler.END


async def ask_delivery(message):
    await message.reply_text(
        "🎁 أرسل <b>ما سيستلمه الزبون بعد الدفع</b>:\n"
        "• نص (رابط / كود / تعليمات) — آخر نص يستبدل السابق\n"
        "• و/أو ملف (أرسله كـ Document)\n\n"
        "عند الانتهاء اكتب /done."
    )


async def photos_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    p = get_product(a["pid"])
    if a.get("mode") == "new" and p and not p["shipping"]:
        await ask_delivery(update.message)
        return A_DELIV
    return await finish_admin(update, context)


async def got_deliv_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_product(adm(context)["pid"], delivery_text=update.message.text.strip()[:3000])
    await update.message.reply_text("✅ تم حفظ النص. أرسل ملفاً أيضاً أو اكتب /done.")
    return A_DELIV


async def got_deliv_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_product(adm(context)["pid"], delivery_file=update.message.document.file_id)
    await update.message.reply_text("✅ تم حفظ الملف. أرسل نصاً أيضاً أو اكتب /done.")
    return A_DELIV


async def deliv_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await finish_admin(update, context)


# ── تعديل منتج (دخول من الأزرار) ──
EDIT_PROMPTS = {
    "name": "✏️ اكتب الاسم الجديد:",
    "desc": "📝 اكتب الوصف الجديد (أو - لمسحه):",
    "price": "💰 اكتب السعر الجديد بالنجوم:",
    "stock": "📦 اكتب المخزون الجديد (للمنتج الرقمي: 0 = غير محدود):",
}


async def edit_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        return ConversationHandler.END
    _, field, pid = q.data.split("_")
    pid = int(pid)
    if not get_product(pid):
        await q.message.reply_text("المنتج غير موجود.")
        return ConversationHandler.END
    context.user_data["adm"] = {"mode": "edit", "pid": pid, "field": field}

    if field == "photos":
        context.user_data["adm"]["replace"] = True
        await q.message.reply_text(
            f"📷 أرسل الصور/الفيديوهات الجديدة (حتى {MAX_PHOTOS}) — ستستبدل القديمة.\nعند الانتهاء: /done  |  للإبقاء على القديمة: /skip"
        )
        return A_PHOTOS
    if field == "deliv":
        await ask_delivery(q.message)
        return A_DELIV
    await q.message.reply_text(EDIT_PROMPTS[field] + "\n(للإلغاء: /cancel)")
    return E_VALUE


async def got_edit_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    pid, field = a["pid"], a["field"]
    p = get_product(pid)
    if not p:
        await update.message.reply_text("المنتج غير موجود.")
        return ConversationHandler.END
    t = update.message.text.strip()

    if field == "name":
        if not (2 <= len(t) <= 100):
            await update.message.reply_text("⚠️ اكتب اسماً من 2 إلى 100 حرف.")
            return E_VALUE
        update_product(pid, name=t)
    elif field == "desc":
        update_product(pid, description="" if t == "-" else t[:700])
    elif field == "price":
        if not t.isdigit() or not (1 <= int(t) <= 1000000):
            await update.message.reply_text("⚠️ اكتب رقماً صحيحاً أكبر من 0.")
            return E_VALUE
        update_product(pid, price=int(t))
    elif field == "stock":
        n = parse_stock(t, p["shipping"])
        if n is None:
            await update.message.reply_text("⚠️ اكتب رقماً صحيحاً.")
            return E_VALUE
        update_product(pid, stock=n)

    context.user_data.pop("adm", None)
    await update.message.reply_text("✅ تم التحديث.")
    await render_panel(update.message, pid)
    return ConversationHandler.END


async def adm_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = context.user_data.pop("adm", {})
    if a.get("mode") == "new" and a.get("pid"):
        delete_product(a["pid"])  # مسودة غير مكتملة
    await update.message.reply_text("تم الإلغاء.")
    return ConversationHandler.END


# ───────────────────────── أوامر الأدمن: الطلبات ─────────────────────────
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
    if not is_admin(update):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("الاستعمال: <code>/shipped رقم_الطلب</code>")
        return
    o = get_order(int(context.args[0]))
    if not o or o["status"] != "paid":
        await update.message.reply_text("الطلب غير موجود أو غير مدفوع.")
        return
    set_order(o["id"], "shipped")
    await update.message.reply_text(f"✅ الطلب #{o['id']} أصبح «تم الشحن».")
    if o.get("wilaya"):
        try:
            await context.bot.send_message(o["user_id"], f"🚚 طلبك #{o['id']} في الطريق إليك! سيتصل بك المُوصِّل قريباً.")
        except Exception:
            pass


async def cmd_refund(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("الاستعمال: <code>/refund رقم_الطلب</code>")
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
    if o["status"] == "paid" and o.get("product_id"):
        restock_one(o["product_id"])
    set_order(o["id"], "refunded")
    await update.message.reply_text(f"💸 تم استرجاع {o['amount']}⭐ للطلب #{o['id']}.")
    try:
        await context.bot.send_message(o["user_id"], f"💸 تم استرجاع نجوم الطلب #{o['id']} إلى حسابك.")
    except Exception:
        pass


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    t = (
        "🛠 <b>أوامر الأدمن</b>\n\n"
        "/addproduct — إضافة منتج جديد\n"
        "/products — إدارة المنتجات (تعديل، صور، شحن/رقمي، مخزون، إخفاء، حذف)\n"
        "/orders — الطلبات المدفوعة\n"
        "/shipped رقم — تم الشحن\n"
        "/refund رقم — استرجاع النجوم"
    )
    if shop_id() == 0:
        t += (
            "\n\n🤖 <b>البوتات المتعددة</b>\n"
            "أرسل هنا <b>توكن بوت جديد</b> (من @BotFather) لربطه وتشغيله فوراً.\n"
            "/bots — البوتات المرتبطة\n"
            "/delbot ID — فصل بوت وإيقافه"
        )
    await update.message.reply_text(t)


# ───────────────────────── إدارة البوتات المتعددة (البوت الرئيسي فقط) ─────────────────────────
async def got_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """الأدمن يرسل توكن بوت جديد ← يُتحقق منه ويُحفظ ويُشغَّل."""
    if shop_id() != 0 or not is_admin(update):
        return
    token = update.message.text.strip()
    # احذف رسالة التوكن من المحادثة للأمان
    try:
        await update.message.delete()
    except Exception:
        pass
    chat = update.effective_chat.id

    if token == BOT_TOKEN:
        await context.bot.send_message(chat, "⚠️ هذا توكن البوت الرئيسي نفسه.")
        return
    try:
        async with Bot(token) as b:
            me = await b.get_me()
    except Exception as e:
        await context.bot.send_message(chat, f"❌ التوكن غير صالح: {esc(str(e)[:150])}")
        return

    owner = update.effective_user.id
    # إن كان يعمل مسبقاً أعد تشغيله بالتوكن الجديد
    if me.id in RUNNING:
        await stop_shop(me.id)
    with cursor() as cur:
        cur.execute("SELECT bot_id FROM shop_bots WHERE bot_id=?", (me.id,))
        exists = cur.fetchone()
        if exists:
            cur.execute(
                "UPDATE shop_bots SET token=?, username=?, owner_id=?, active=1 WHERE bot_id=?",
                (token, me.username, owner, me.id),
            )
        else:
            cur.execute(
                "INSERT INTO shop_bots (bot_id, token, username, owner_id, active, created_at) VALUES (?,?,?,?,1,?)",
                (me.id, token, me.username, owner, int(time.time())),
            )
    try:
        await start_shop(token, {"id": me.id, "admin": owner, "username": me.username})
    except Exception as e:
        log.error("start shop failed: %s", e)
        await context.bot.send_message(chat, f"❌ تعذّر تشغيل البوت: {esc(str(e)[:200])}")
        return

    await context.bot.send_message(
        chat,
        f"✅ تم ربط وتشغيل البوت <b>@{esc(me.username)}</b> (ID: <code>{me.id}</code>)\n\n"
        f"🔗 افتحه: https://t.me/{esc(me.username)}\n"
        "اضغط /start هناك — أنت أدمنه، ثم استعمل /addproduct لإضافة منتجاتك.\n\n"
        "ℹ️ منتجاته وطلباته منفصلة تماماً عن هذا البوت.\n"
        "🗑 لفصله لاحقاً: <code>/delbot " + str(me.id) + "</code>",
    )


async def cmd_bots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if shop_id() != 0 or not is_admin(update):
        return
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_bots ORDER BY created_at")
        rows = cur.fetchall()
    if not rows:
        await update.message.reply_text("لا توجد بوتات مرتبطة. أرسل توكن بوت لإضافته.")
        return
    t = "🤖 <b>البوتات المرتبطة</b>\n\n"
    for r in rows:
        live = "🟢 يعمل" if r["bot_id"] in RUNNING else ("⚪ متوقف" if r["active"] else "🚫 مفصول")
        t += f"{live} — @{esc(r['username'])} — ID: <code>{r['bot_id']}</code>\n"
    await update.message.reply_text(t)


async def cmd_delbot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if shop_id() != 0 or not is_admin(update):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("الاستعمال: <code>/delbot ID_البوت</code> (انظر /bots)")
        return
    bid = int(context.args[0])
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_bots WHERE bot_id=?", (bid,))
        r = cur.fetchone()
    if not r:
        await update.message.reply_text("لا يوجد بوت بهذا الـ ID.")
        return
    await stop_shop(bid)
    with cursor() as cur:
        cur.execute("UPDATE shop_bots SET active=0 WHERE bot_id=?", (bid,))
    await update.message.reply_text(
        f"🗑 تم إيقاف وفصل @{esc(r['username'])}.\n"
        "(بياناته محفوظة؛ إن أرسلت توكنه مجدداً يعود بمنتجاته.)"
    )


async def bind_shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يعمل أولاً مع كل تحديث ويحدد أي متجر يخدمه."""
    _shop.set(context.application.bot_data["shop"])


async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled error", exc_info=context.error)


ADMIN_COMMANDS = [
    BotCommand("addproduct", "➕ إضافة منتج"),
    BotCommand("products", "🗂 إدارة المنتجات"),
    BotCommand("orders", "📦 الطلبات المدفوعة"),
    BotCommand("shipped", "🚚 تم الشحن (رقم)"),
    BotCommand("refund", "💸 استرجاع (رقم)"),
    BotCommand("help", "🛠 أوامر الأدمن"),
    BotCommand("start", "🏠 الصفحة الرئيسية"),
]

MAIN_COMMANDS = [
    BotCommand("bots", "🤖 البوتات المرتبطة"),
    BotCommand("delbot", "🗑 فصل بوت (ID)"),
]


async def set_admin_commands(bot, admin_id, is_main=False):
    if not admin_id:
        return
    try:
        cmds = ADMIN_COMMANDS + (MAIN_COMMANDS if is_main else [])
        await bot.set_my_commands(cmds, scope=BotCommandScopeChat(admin_id))
    except Exception as e:
        log.warning("admin set_my_commands failed: %s", e)


async def setup_commands(bot, admin_id, is_main=False):
    try:
        await bot.set_my_commands([BotCommand("start", "🏠 الصفحة الرئيسية")], scope=BotCommandScopeDefault())
    except Exception as e:
        log.warning("default set_my_commands failed: %s", e)
    await set_admin_commands(bot, admin_id, is_main)


# ───────────────────────── تشغيل البوتات الفرعية ─────────────────────────
async def start_shop(token, shop):
    app = build_app(token, shop, is_main=False)
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    RUNNING[shop["id"]] = app
    await setup_commands(app.bot, shop["admin"], False)
    log.info("Shop bot @%s started", shop.get("username"))


async def stop_shop(bid):
    app = RUNNING.pop(bid, None)
    if app:
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except Exception as e:
            log.warning("stop shop failed: %s", e)


async def load_shops():
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_bots WHERE active=1")
        rows = cur.fetchall()
    for r in rows:
        try:
            await start_shop(r["token"], {"id": r["bot_id"], "admin": r["owner_id"], "username": r["username"]})
        except Exception as e:
            log.error("failed to start shop bot %s: %s", r["bot_id"], e)


async def post_init(app: Application):
    await setup_commands(app.bot, ADMIN_ID, True)
    await load_shops()
    if not ADMIN_ID:
        return
    msg = "✅ البوت يعمل.\n"
    if USE_PG:
        msg += "🗄 قاعدة البيانات: PostgreSQL"
    else:
        msg += f"🗄 قاعدة البيانات: SQLite ({os.path.abspath(DB_PATH)})"
        if not os.path.abspath(DB_PATH).startswith("/data"):
            msg += (
                "\n\n⚠️ الملف مؤقت: عند إعادة النشر قد تضيع المنتجات والبوتات المرتبطة. "
                "اربط Volume على /data وضع DB_PATH=/data/shop.db"
            )
    if RUNNING:
        msg += f"\n🤖 بوتات فرعية تعمل: {len(RUNNING)}"
    msg += "\n\nاكتب /help لعرض الأوامر."
    try:
        await app.bot.send_message(ADMIN_ID, msg)
    except Exception as e:
        log.warning("startup notice failed: %s", e)


async def post_shutdown(app: Application):
    for bid in list(RUNNING):
        await stop_shop(bid)


# ───────────────────────── تسجيل الـ Handlers ─────────────────────────
def register_handlers(app: Application, is_main: bool):
    text_only = filters.TEXT & ~filters.COMMAND

    # يحدد المتجر قبل أي handler آخر (group -1)
    app.add_handler(TypeHandler(Update, bind_shop), group=-1)

    admin_conv = ConversationHandler(
        entry_points=[
            CommandHandler("addproduct", cmd_addproduct),
            CallbackQueryHandler(new_product_cb, pattern=r"^adm_new$"),
            CallbackQueryHandler(edit_entry, pattern=r"^edt_(name|desc|price|stock|photos|deliv)_\d+$"),
        ],
        states={
            A_NAME: [MessageHandler(text_only, got_name)],
            A_PRICE: [MessageHandler(text_only, got_price)],
            A_TYPE: [CallbackQueryHandler(got_type, pattern=r"^nt_(ship|dig)$")],
            A_STOCK: [MessageHandler(text_only, got_stock)],
            A_DESC: [
                CommandHandler("skip", skip_desc),
                MessageHandler(text_only, got_desc),
            ],
            A_PHOTOS: [
                MessageHandler(filters.PHOTO | filters.VIDEO, got_photo),
                CommandHandler(["done", "skip"], photos_done),
                MessageHandler(text_only, photos_wrong),
            ],
            A_DELIV: [
                MessageHandler(filters.Document.ALL, got_deliv_file),
                CommandHandler(["done", "skip"], deliv_done),
                MessageHandler(text_only, got_deliv_text),
            ],
            E_VALUE: [MessageHandler(text_only, got_edit_value)],
        },
        fallbacks=[
            CommandHandler("cancel", adm_cancel),
            CommandHandler("start", start),
        ],
        allow_reentry=True,
    )

    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(buy_cb, pattern=r"^buy_\d+$")],
        states={
            PHONE: [
                MessageHandler(filters.CONTACT, got_phone),
                MessageHandler(text_only, phone_text),
            ],
            WILAYA: [CallbackQueryHandler(got_wilaya, pattern=r"^w_\d+$")],
            ADDRESS: [MessageHandler(text_only, got_address)],
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

    app.add_handler(admin_conv)
    app.add_handler(conv)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(prod_cb, pattern=r"^prod_\d+$"))
    app.add_handler(CallbackQueryHandler(list_cb, pattern=r"^list$"))
    app.add_handler(PreCheckoutQueryHandler(pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, on_paid))

    app.add_handler(CallbackQueryHandler(adm_list_cb, pattern=r"^adm_list$"))
    app.add_handler(CallbackQueryHandler(adm_cb, pattern=r"^adm_(panel|ship|active|clrphotos|del|delyes)_\d+$"))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("products", cmd_products))
    app.add_handler(CommandHandler("orders", cmd_orders))
    app.add_handler(CommandHandler("shipped", cmd_shipped))
    app.add_handler(CommandHandler("refund", cmd_refund))

    if is_main:
        app.add_handler(CommandHandler("bots", cmd_bots))
        app.add_handler(CommandHandler("delbot", cmd_delbot))
        # أي رسالة نصية على شكل توكن بوت (تُعالَج فقط إن لم تكن داخل محادثة نشطة)
        app.add_handler(MessageHandler(filters.TEXT & filters.Regex(TOKEN_RE), got_token))

    app.add_error_handler(on_error)


def build_app(token, shop, is_main=False):
    b = Application.builder().token(token).defaults(Defaults(parse_mode=ParseMode.HTML))
    if is_main:
        b = b.post_init(post_init).post_shutdown(post_shutdown)
    app = b.build()
    app.bot_data["shop"] = shop
    register_handlers(app, is_main)
    return app


# ───────────────────────── تشغيل ─────────────────────────
def main():
    init_db()
    app = build_app(BOT_TOKEN, {"id": 0, "admin": ADMIN_ID, "username": None}, is_main=True)
    log.info("Bot started (polling)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
