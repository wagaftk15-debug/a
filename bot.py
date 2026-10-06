import os
import html
import logging
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
log = logging.getLogger("shop-bot")

# ───────────────────────── الإعدادات ─────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
ADMIN_ID = int(os.environ.get("ADMIN_CHAT_ID", "0"))

PRODUCT_NAME = os.environ.get("PRODUCT_NAME", "آلة لحام بلاستيك PFS-300")
PRODUCT_DESC = os.environ.get("PRODUCT_DESC", "آلة لحام بلاستيك PFS-300 — جاهزة للاستعمال.")
PRODUCT_PHOTO_URL = os.environ.get("PRODUCT_PHOTO_URL", "")  # اختياري: رابط صورة أو file_id
PRICE_STARS = int(os.environ.get("PRICE_STARS", "5000"))
RESERVE_MINUTES = 10

PHONE, WILAYA, ADDRESS, CONFIRM = range(4)

WILAYAS = [
    "أدرار", "الشلف", "الأغواط", "أم البواقي", "باتنة", "بجاية", "بسكرة", "بشار", "البليدة", "البويرة",
    "تمنراست", "تبسة", "تلمسان", "تيارت", "تيزي وزو", "الجزائر", "الجلفة", "جيجل", "سطيف", "سعيدة",
    "سكيكدة", "سيدي بلعباس", "عنابة", "قالمة", "قسنطينة", "المدية", "مستغانم", "المسيلة", "معسكر", "ورقلة",
    "وهران", "البيض", "إليزي", "برج بوعريريج", "بومرداس", "الطارف", "تندوف", "تيسمسيلت", "الوادي", "خنشلة",
    "سوق أهراس", "تيبازة", "ميلة", "عين الدفلى", "النعامة", "عين تموشنت", "غرداية", "غليزان", "تيميمون",
    "برج باجي مختار", "أولاد جلال", "بني عباس", "عين صالح", "عين قزام", "تقرت", "جانت", "المغير", "المنيعة",
]

# ───────────────────────── قاعدة البيانات ─────────────────────────
pool = None


@contextmanager
def cursor():
    conn = pool.getconn()
    try:
        with conn:  # commit / rollback تلقائي
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
    finally:
        pool.putconn(conn)


RESERVE_SQL = """
    UPDATE shop_product
    SET reserved_by = %s, reserved_until = NOW() + (%s * INTERVAL '1 minute')
    WHERE id = 1 AND stock > 0
      AND (reserved_by IS NULL OR reserved_until < NOW() OR reserved_by = %s)
    RETURNING id
"""


def init_db():
    global pool
    pool = ThreadedConnectionPool(1, 8, DATABASE_URL, connect_timeout=10)
    with cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shop_product (
                id INT PRIMARY KEY,
                stock INT NOT NULL DEFAULT 1,
                reserved_by BIGINT,
                reserved_until TIMESTAMPTZ
            )
        """)
        cur.execute("INSERT INTO shop_product (id, stock) VALUES (1, 1) ON CONFLICT DO NOTHING")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shop_orders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                full_name TEXT,
                phone TEXT,
                wilaya TEXT,
                address TEXT,
                amount INT,
                status TEXT DEFAULT 'pending',
                charge_id TEXT UNIQUE,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                paid_at TIMESTAMPTZ
            )
        """)
    log.info("DB ready")


def availability(user_id):
    """available / held (محجوزة لزبون آخر) / sold"""
    with cursor() as cur:
        cur.execute("""
            SELECT stock,
                   (reserved_by IS NOT NULL AND reserved_until > NOW() AND reserved_by <> %s) AS held
            FROM shop_product WHERE id = 1
        """, (user_id,))
        r = cur.fetchone()
    if not r or r["stock"] <= 0:
        return "sold"
    return "held" if r["held"] else "available"


def reserve(user_id):
    with cursor() as cur:
        cur.execute(RESERVE_SQL, (user_id, RESERVE_MINUTES, user_id))
        return cur.fetchone() is not None


def create_order(user, phone, wilaya, address):
    with cursor() as cur:
        cur.execute("UPDATE shop_orders SET status='cancelled' WHERE user_id=%s AND status='pending'", (user.id,))
        cur.execute("""
            INSERT INTO shop_orders (user_id, username, full_name, phone, wilaya, address, amount)
            VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
        """, (user.id, user.username or "", user.full_name or "", phone, wilaya, address, PRICE_STARS))
        return cur.fetchone()["id"]


def hold_for_payment(order_id, user_id):
    """يُستدعى في pre_checkout: يتأكد أن الطلب صالح ويمدّد الحجز."""
    with cursor() as cur:
        cur.execute("SELECT status, user_id FROM shop_orders WHERE id=%s", (order_id,))
        o = cur.fetchone()
        if not o or o["status"] != "pending" or o["user_id"] != user_id:
            return False
        cur.execute(RESERVE_SQL, (user_id, RESERVE_MINUTES, user_id))
        return cur.fetchone() is not None


def mark_paid(order_id, charge_id):
    """returns (status, order) : ok / soldout / dup / invalid"""
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_orders WHERE id=%s FOR UPDATE", (order_id,))
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
            UPDATE shop_orders SET status='paid', charge_id=%s, paid_at=NOW()
            WHERE id=%s RETURNING *
        """, (charge_id, order_id))
        return "ok", cur.fetchone()


def set_order(order_id, status, charge_id=None):
    with cursor() as cur:
        if charge_id:
            cur.execute("UPDATE shop_orders SET status=%s, charge_id=%s WHERE id=%s", (status, charge_id, order_id))
        else:
            cur.execute("UPDATE shop_orders SET status=%s WHERE id=%s", (status, order_id))


def get_order(order_id):
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_orders WHERE id=%s", (order_id,))
        return cur.fetchone()


def list_orders(limit=10):
    with cursor() as cur:
        cur.execute("""
            SELECT * FROM shop_orders
            WHERE status IN ('paid','shipped','refunded')
            ORDER BY id DESC LIMIT %s
        """, (limit,))
        return cur.fetchall()


def add_stock(n):
    with cursor() as cur:
        cur.execute("UPDATE shop_product SET stock=%s, reserved_by=NULL, reserved_until=NULL WHERE id=1", (n,))


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
    text = (
        f"🛠 <b>{esc(PRODUCT_NAME)}</b>\n\n"
        f"{esc(PRODUCT_DESC)}\n\n"
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

    if PRODUCT_PHOTO_URL:
        try:
            await update.message.reply_photo(PRODUCT_PHOTO_URL, caption=text, reply_markup=markup)
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
    return update.effective_user and update.effective_user.id == ADMIN_ID


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


async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled error", exc_info=context.error)


# ───────────────────────── تشغيل ─────────────────────────
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).defaults(Defaults(parse_mode=ParseMode.HTML)).build()

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
    app.add_handler(CommandHandler("orders", cmd_orders))
    app.add_handler(CommandHandler("shipped", cmd_shipped))
    app.add_handler(CommandHandler("refund", cmd_refund))
    app.add_handler(CommandHandler("stock", cmd_stock))
    app.add_error_handler(on_error)

    log.info("Bot started (polling)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
