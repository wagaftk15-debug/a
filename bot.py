import os
import sys
import re
import html
import json
import hmac
import base64
import hashlib
from types import SimpleNamespace
from urllib.parse import parse_qsl
import time
import asyncio
import sqlite3
import logging
import threading
from contextlib import contextmanager
from contextvars import ContextVar

import httpx
try:
    from aiohttp import web
except ImportError:          # البوت يعمل بدون الموقع إذا لم تُثبَّت aiohttp
    web = None
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool, PoolError
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, LabeledPrice,
    BotCommand, BotCommandScopeChat, BotCommandScopeDefault, InputMediaPhoto, InputMediaVideo,
    Bot, WebAppInfo, MenuButtonWebApp,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ConversationHandler, PreCheckoutQueryHandler, ContextTypes, Defaults, TypeHandler, filters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", stream=sys.stdout)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("shop-bot")


# ═════════════════════════════════════════════════════════════════════════════
# 1) الإعدادات
# ═════════════════════════════════════════════════════════════════════════════
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ.get("ADMIN_CHAT_ID", "0") or 0)
DB_PATH = os.environ.get("DB_PATH", "shop.db")

# ── TON ──
TON_WALLET = os.environ.get("TON_WALLET", "")
TONCENTER_KEY = os.environ.get("TONCENTER_KEY", "")
WALLET_RE = re.compile(r"^(EQ|UQ)[A-Za-z0-9_-]{46}$")
TOKEN_RE = re.compile(r"^\s*\d{6,12}:[A-Za-z0-9_-]{30,}\s*$")

RESERVE_MINUTES = 10
MAX_PHOTOS = 10
MAX_EXTRA_LEN = 100
MAX_EXTRAS = 10

# ── الموقع (Telegram Mini App) ──
WEBAPP_URL = os.environ.get("WEBAPP_URL", "").rstrip("/")
try:
    PORT = int(os.environ.get("PORT", "8080") or 8080)
except ValueError:
    PORT = 8080
_raw = (WEBAPP_URL or os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")).strip().rstrip("/")
if _raw.startswith("http://"):
    _raw = "https://" + _raw[len("http://"):]
elif _raw and not _raw.startswith("https://"):
    _raw = "https://" + _raw
WEBAPP_URL = _raw
LOGO_PATH = os.environ.get("LOGO_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "shop_logo.png"))

LANGS = ("ar", "en")
DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "ar").lower()
if DEFAULT_LANG not in LANGS:
    DEFAULT_LANG = "ar"

# حالات محادثة الزبون
PHONE, WILAYA, ADDRESS, CONFIRM, EXTRA = range(5)
# حالات محادثة الأدمن
A_NAME, A_PRICE, A_TYPE, A_STOCK, A_DESC, A_PHOTOS, A_DELIV, E_VALUE, X_LABEL, X_TYPE = range(20, 30)
W_ADDR, A_TON, A_METHOD = 30, 31, 32
X_LEN, X_PRE, X_SUF = 33, 34, 35
M_UP = 36
LIB_PAGE = 8

AR_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")


# ═════════════════════════════════════════════════════════════════════════════
# 2) القواميس: الولايات والنصوص المترجمة
# ═════════════════════════════════════════════════════════════════════════════
WILAYAS = {
    "ar": [
        "أدرار", "الشلف", "الأغواط", "أم البواقي", "باتنة", "بجاية", "بسكرة", "بشار", "البليدة", "البويرة",
        "تمنراست", "تبسة", "تلمسان", "تيارت", "تيزي وزو", "الجزائر", "الجلفة", "جيجل", "سطيف", "سعيدة",
        "سكيكدة", "سيدي بلعباس", "عنابة", "قالمة", "قسنطينة", "المدية", "مستغانم", "المسيلة", "معسكر", "ورقلة",
        "وهران", "البيض", "إليزي", "برج بوعريريج", "بومرداس", "الطارف", "تندوف", "تيسمسيلت", "الوادي", "خنشلة",
        "سوق أهراس", "تيبازة", "ميلة", "عين الدفلى", "النعامة", "عين تموشنت", "غرداية", "غليزان", "تيميمون",
        "برج باجي مختار", "أولاد جلال", "بني عباس", "عين صالح", "عين قزام", "تقرت", "جانت", "المغير", "المنيعة",
    ],
    "en": [
        "Adrar", "Chlef", "Laghouat", "Oum El Bouaghi", "Batna", "Béjaïa", "Biskra", "Béchar", "Blida", "Bouira",
        "Tamanrasset", "Tébessa", "Tlemcen", "Tiaret", "Tizi Ouzou", "Algiers", "Djelfa", "Jijel", "Sétif", "Saïda",
        "Skikda", "Sidi Bel Abbès", "Annaba", "Guelma", "Constantine", "Médéa", "Mostaganem", "M'Sila", "Mascara", "Ouargla",
        "Oran", "El Bayadh", "Illizi", "Bordj Bou Arréridj", "Boumerdès", "El Tarf", "Tindouf", "Tissemsilt", "El Oued", "Khenchela",
        "Souk Ahras", "Tipaza", "Mila", "Aïn Defla", "Naâma", "Aïn Témouchent", "Ghardaïa", "Relizane", "Timimoun",
        "Bordj Badji Mokhtar", "Ouled Djellal", "Béni Abbès", "In Salah", "In Guezzam", "Touggourt", "Djanet", "El M'Ghair", "El Menia",
    ],
}

# كل نص يراه المستخدم أو الأدمن موجود هنا فقط: (المفتاح، العربية، English)
_S = [
    # ── عام ──
    ("no_products", "🚧 لا توجد منتجات حالياً.", "🚧 No products available right now."),
    ("admin_add_hint", "\n\nأنت الأدمن: اكتب /addproduct لإضافة أول منتج.",
     "\n\nYou are the admin: send /addproduct to add your first product."),
    ("lang_pick", "🌐 اختر لغة المتجر (تُطبَّق على الجميع):\nChoose the shop language (applies to everyone):",
     "🌐 اختر لغة المتجر (تُطبَّق على الجميع):\nChoose the shop language (applies to everyone):"),
    ("lang_set", "✅ تم تغيير لغة المتجر إلى العربية للجميع.", "✅ Shop language changed to English for everyone."),
    ("lang_btn_ar", "🇩🇿 العربية", "🇩🇿 العربية"),
    ("lang_btn_en", "🇬🇧 English", "🇬🇧 English"),
    ("btn_all_products", "🔙 كل المنتجات", "🔙 All products"),
    ("list_title", "🛍 <b>منتجاتنا</b>\n\nاختر منتجاً لعرض تفاصيله 👇",
     "🛍 <b>Our products</b>\n\nPick a product to see its details 👇"),
    ("product_gone", "هذا المنتج لم يعد متوفراً.", "This product is no longer available."),
    ("price_line", "💰 السعر: <b>{price} ⭐</b> (نجوم تيليجرام)\n", "💰 Price: <b>{price} ⭐</b> (Telegram Stars)\n"),
    ("price_ton_line", "💎 السعر: <b>{ton} TON</b>\n", "💎 Price: <b>{ton} TON</b>\n"),
    ("stock_line", "📦 المتوفر: <b>{n}</b>\n", "📦 In stock: <b>{n}</b>\n"),
    ("dz_only", "🇩🇿 البيع والتوصيل داخل الجزائر فقط\n", "🇩🇿 Sales and delivery inside Algeria only\n"),
    ("digital_note", "💾 منتج رقمي — يصلك مباشرة بعد الدفع\n", "💾 Digital product — delivered right after payment\n"),
    ("buy_prompt", "اضغط الزر أدناه لإتمام الطلب 👇", "Tap the button below to order 👇"),
    ("btn_buy", "🛒 اشتري الآن", "🛒 Buy now"),
    ("unavail_sold", "❌ للأسف، نفدت الكمية.", "❌ Sorry, this item is sold out."),
    ("unavail_held", "⏳ القطعة محجوزة حالياً لزبون آخر (يدفع الآن). جرّب بعد عدة دقائق، فقد تعود متاحة.",
     "⏳ This item is currently reserved for another customer (paying now). Try again in a few minutes; it may become available."),
    ("unlimited", "غير محدود", "Unlimited"),
    ("unknown_product", "منتج", "Product"),

    # ── حالات الطلب ──
    ("status_pending", "قيد الانتظار", "Pending"),
    ("status_paid", "مدفوع", "Paid"),
    ("status_shipped", "تم الشحن", "Shipped"),
    ("status_refunded", "مسترجع", "Refunded"),
    ("status_cancelled", "ملغى", "Cancelled"),

    # ── خطوات الطلب ──
    ("btn_cancel", "❌ إلغاء", "❌ Cancel"),
    ("btn_pay", "⭐ الدفع بالنجوم ({price})", "⭐ Pay with Stars ({price})"),
    ("btn_pay_ton", "💎 الدفع بـ TON ({ton})", "💎 Pay with TON ({ton})"),
    ("reserved_ton", "⏳ تم حجز طلبك لمدة {m} دقائق. أكمل الدفع بعملة TON من الرسالة أدناه 👇",
     "⏳ Your order is reserved for {m} minutes. Complete the TON payment using the message below 👇"),
    ("btn_edit_addr", "✏️ تعديل الولاية/العنوان", "✏️ Edit wilaya/address"),
    ("summary_title", "📋 <b>راجع طلبك:</b>\n\n🛍 {name}\n", "📋 <b>Review your order:</b>\n\n🛍 {name}\n"),
    ("summary_amount", "\n💰 المبلغ: <b>{amount}</b>", "\n💰 Amount: <b>{amount}</b>"),
    ("phone_ask",
     "🇩🇿 للتأكد أنك من الجزائر، شارك رقم هاتفك بالضغط على الزر أدناه.\n(يجب أن يكون الرقم جزائري +213 ومرتبط بحسابك)",
     "🇩🇿 To confirm you are in Algeria, share your phone number using the button below.\n(It must be an Algerian +213 number linked to your account)"),
    ("btn_share_phone", "📱 مشاركة رقم هاتفي", "📱 Share my phone number"),
    ("phone_not_yours", "⚠️ يرجى مشاركة رقمك أنت، وليس رقم شخص آخر.", "⚠️ Please share your own number, not someone else's."),
    ("phone_not_dz", "🚫 عذراً، البيع متاح فقط لأصحاب أرقام الهاتف الجزائرية (+213).",
     "🚫 Sorry, sales are only available to Algerian phone numbers (+213)."),
    ("phone_ok", "✅ تم التحقق من رقمك.", "✅ Your number is verified."),
    ("pick_wilaya", "📍 اختر ولايتك:", "📍 Choose your wilaya (province):"),
    ("phone_hint", "اضغط على زر «📱 مشاركة رقم هاتفي» أسفل الشاشة، أو /cancel للإلغاء.",
     "Tap the “📱 Share my phone number” button at the bottom of the screen, or /cancel to cancel."),
    ("wilaya_chosen",
     "📍 الولاية: <b>{w}</b>\n\n🏠 الآن اكتب عنوانك الكامل للتوصيل:\nالبلدية، الحي، الشارع، رقم المنزل، وأي علامة مميزة.",
     "📍 Wilaya: <b>{w}</b>\n\n🏠 Now type your full delivery address:\nMunicipality, neighborhood, street, house number, and any landmark."),
    ("addr_bad", "⚠️ العنوان قصير جداً أو طويل جداً. اكتب عنواناً واضحاً (10 – 300 حرف).",
     "⚠️ The address is too short or too long. Write a clear address (10–300 characters)."),
    ("extra_hint_num", "(أرسل أرقاماً فقط)", "(send digits only)"),
    ("extra_hint_text", "(أرسل نصاً)", "(send text)"),
    ("cancel_hint", "(للإلغاء: /cancel)", "(to cancel: /cancel)"),
    ("extra_bad_num", "⚠️ أرسل أرقاماً فقط (بدون حروف أو رموز).", "⚠️ Send digits only (no letters or symbols)."),
    ("extra_bad_text", "⚠️ اكتب نصاً من 1 إلى 300 حرف.", "⚠️ Write text between 1 and 300 characters."),
    ("extra_bad_len", "⚠️ يجب أن يكون الطول <b>{n}</b> بالضبط (أرسلت {got}).",
     "⚠️ The length must be exactly <b>{n}</b> (you sent {got})."),
    ("extra_bad_pre", "⚠️ يجب أن يبدأ بـ <code>{v}</code>.", "⚠️ It must start with <code>{v}</code>."),
    ("extra_bad_suf", "⚠️ يجب أن ينتهي بـ <code>{v}</code>.", "⚠️ It must end with <code>{v}</code>."),
    ("rule_len_num", "📏 الطول: {n} رقماً", "📏 Length: {n} digits"),
    ("rule_len_text", "📏 الطول: {n} حرفاً", "📏 Length: {n} characters"),
    ("rule_pre", "▶️ يبدأ بـ: <code>{v}</code>", "▶️ Starts with: <code>{v}</code>"),
    ("rule_suf", "⏹ ينتهي بـ: <code>{v}</code>", "⏹ Ends with: <code>{v}</code>"),
    ("session_expired", "انتهت الجلسة، اكتب /start للبدء من جديد.", "Session expired. Send /start to begin again."),
    ("reserved", "⏳ تم حجز طلبك لمدة {m} دقائق. أكمل الدفع من الفاتورة أدناه 👇",
     "⏳ Your order is reserved for {m} minutes. Complete the payment using the invoice below 👇"),
    ("invoice_ship_desc", "{name} — توصيل داخل الجزائر", "{name} — delivery inside Algeria"),
    ("cancelled", "تم إلغاء الطلب. اكتب /start للبدء من جديد.", "Order cancelled. Send /start to begin again."),

    # ── الدفع ──
    ("precheck_fail", "عذراً، المنتج لم يعد متاحاً أو انتهت مهلة الحجز.",
     "Sorry, the product is no longer available or the reservation expired."),
    ("your_product", "🎁 <b>منتجك:</b>\n\n{text}", "🎁 <b>Your product:</b>\n\n{text}"),
    ("paid_ok_digital", "✅ <b>تم الدفع بنجاح!</b>\n\nرقم طلبك: <b>#{oid}</b>",
     "✅ <b>Payment successful!</b>\n\nYour order number: <b>#{oid}</b>"),
    ("will_send", "سيصلك المنتج من المشرف قريباً 🙏", "The admin will send you the product shortly 🙏"),
    ("paid_ok_ship",
     "✅ <b>تم الدفع بنجاح!</b>\n\nرقم طلبك: <b>#{oid}</b>\nسنتواصل معك على رقمك لتأكيد التوصيل قريباً. شكراً لثقتك 🙏",
     "✅ <b>Payment successful!</b>\n\nYour order number: <b>#{oid}</b>\nWe will contact you on your number to confirm delivery soon. Thank you for your trust 🙏"),
    ("refunded_soldout", "⚠️ نعتذر، المنتج بيع للتو. تم استرجاع نجومك بالكامل ✅",
     "⚠️ Sorry, the product was just sold. Your stars have been fully refunded ✅"),
    ("refund_problem", "⚠️ حدثت مشكلة، سيتواصل معك المشرف لاسترجاع نجومك.",
     "⚠️ A problem occurred; the admin will contact you to refund your stars."),
    ("shipped_user", "🚚 طلبك #{id} في الطريق إليك! سيتصل بك المُوصِّل قريباً.",
     "🚚 Your order #{id} is on its way! The courier will contact you soon."),
    ("refunded_user", "💸 تم استرجاع نجوم الطلب #{id} إلى حسابك.", "💸 The stars for order #{id} have been refunded to your account."),

    # ── TON ──
    ("btn_ton_open", "💎 فتح Tonkeeper والدفع", "💎 Open Tonkeeper & pay"),
    ("btn_ton_check", "🔄 تحقق من الدفع", "🔄 Check payment"),
    ("ton_pay_msg",
     "💎 <b>الدفع بعملة TON</b>\n\nالمبلغ: <b>{amt} TON</b>\nالعنوان:\n<code>{addr}</code>\nالتعليق (إلزامي، لا تغيّره):\n<code>{memo}</code>\n\nاضغط الزر لفتح Tonkeeper. بعد التحويل يتأكد البوت تلقائياً (أو اضغط «تحقق»).",
     "💎 <b>Pay with TON</b>\n\nAmount: <b>{amt} TON</b>\nAddress:\n<code>{addr}</code>\nComment (required, do not change):\n<code>{memo}</code>\n\nTap the button to open Tonkeeper. The bot confirms automatically after the transfer (or tap “Check”)."),
    ("ton_not_found", "⏳ لم يصل التحويل بعد. انتظر قليلاً (قد يستغرق دقيقة) ثم أعد المحاولة.",
     "⏳ Transfer not received yet. Wait a bit (it may take a minute) and try again."),
    ("ton_already", "✅ هذا الطلب مدفوع مسبقاً.", "✅ This order is already paid."),
    ("ton_invalid_order", "الطلب لم يعد صالحاً.", "This order is no longer valid."),
    ("ton_manual_user", "⚠️ وصل تحويلك لكن الطلب لم يعد متاحاً. سيتواصل معك المشرف لاسترجاع المبلغ.",
     "⚠️ Your transfer arrived but the order is no longer available. The admin will contact you for a refund."),
    ("adm_ton_manual",
     "🚨 <b>دفع TON يحتاج تدخلاً يدوياً</b>\nالطلب: #{oid}\nالمستخدم: <code>{uid}</code>\nالهاش: <code>{h}</code>\nالحالة: {why}",
     "🚨 <b>TON payment needs manual handling</b>\nOrder: #{oid}\nUser: <code>{uid}</code>\nHash: <code>{h}</code>\nStatus: {why}"),
    ("pt_ton", "💎 سعر TON: {p}", "💎 TON price: {p}"),
    ("b_ton", "💎 سعر TON", "💎 TON price"),
    ("ep_ton", "💎 اكتب السعر بعملة TON (مثال: 1.5) أو - لإلغاء الدفع بـ TON:",
     "💎 Type the price in TON (e.g. 1.5) or - to disable TON payment:"),
    ("bad_ton", "⚠️ اكتب رقماً صالحاً (مثل 0.5).", "⚠️ Enter a valid number (e.g. 0.5)."),
    ("wallet_usage", "الاستعمال: <code>/setwallet عنوان_المحفظة</code>\nالمحفظة الحالية: <code>{w}</code>",
     "Usage: <code>/setwallet WALLET_ADDRESS</code>\nCurrent wallet: <code>{w}</code>"),
    ("wallet_bad", "⚠️ عنوان محفظة غير صالح (يبدأ بـ UQ أو EQ).", "⚠️ Invalid wallet address (starts with UQ or EQ)."),
    ("wallet_ok", "✅ تم حفظ محفظة TON لهذا المتجر.", "✅ TON wallet saved for this shop."),
    ("b_wallet", "💎 محفظة TON", "💎 TON wallet"),
    ("wallet_ask",
     "💎 أرسل الآن <b>عنوان محفظة TON</b> الخاصة بك (يبدأ بـ UQ أو EQ).\nمن Tonkeeper: اضغط Receive ثم انسخ العنوان.\nالحالية: <code>{w}</code>\n(للإلغاء: /cancel)",
     "💎 Now send your <b>TON wallet address</b> (starts with UQ or EQ).\nIn Tonkeeper: tap Receive and copy the address.\nCurrent: <code>{w}</code>\n(to cancel: /cancel)"),
    ("ask_ton", "💎 اكتب سعر المنتج بعملة TON (مثال: 1.5):",
     "💎 Type the product price in TON (e.g. 1.5):"),
    ("pick_method", "💳 اختر <b>طريقة الدفع</b> لهذا المنتج:", "💳 Choose the <b>payment method</b> for this product:"),
    ("b_pm_stars", "⭐ نجوم تيليجرام فقط", "⭐ Telegram Stars only"),
    ("b_pm_ton", "💎 TON فقط", "💎 TON only"),
    ("b_pm_both", "⭐ + 💎 الاثنان معاً", "⭐ + 💎 Both"),
    ("wallet_needed", "⚠️ لاستعمال TON أضف محفظتك أولاً بالأمر /setwallet.",
     "⚠️ To use TON, add your wallet first with /setwallet."),
    ("need_one", "⚠️ يجب أن تبقى طريقة دفع واحدة على الأقل.", "⚠️ At least one payment method must remain."),
    ("no_pay_method", "⚠️ لا توجد طريقة دفع متاحة لهذا المنتج حالياً.",
     "⚠️ No payment method is available for this product right now."),
    ("pt_stars_off", "⭐ الدفع بالنجوم: 🚫 معطّل", "⭐ Stars payment: 🚫 Disabled"),
    ("pt_ton_off", "💎 الدفع بـ TON: 🚫 معطّل", "💎 TON payment: 🚫 Disabled"),
    ("b_off_stars", "🗑 حذف دفع النجوم", "🗑 Remove Stars"),
    ("b_off_ton", "🗑 حذف دفع TON", "🗑 Remove TON"),
    ("ton_no_pending", "لا يوجد طلب دفع TON معلّق لك. اضغط «اشتري الآن» من /start.",
     "You have no pending TON order. Tap “Buy now” from /start."),
    ("paid_words", "دفعت|حولت|حوّلت|تم الدفع|تم التحويل", "paid|sent|transferred|done|payment done"),
    ("help_ton", "\n/setwallet عنوان — محفظة TON لاستلام الدفع", "\n/setwallet address — TON wallet to receive payments"),

    # ── إشعارات الأدمن ──
    ("adm_digital_nodeliv", "⚠️ طلب رقمي #{oid} بلا محتوى تسليم! أرسله للزبون يدوياً (ID: <code>{uid}</code>).",
     "⚠️ Digital order #{oid} has no delivery content! Send it to the customer manually (ID: <code>{uid}</code>)."),
    ("adm_new_paid", "🔔 <b>طلب مدفوع جديد!</b>\n\n", "🔔 <b>New paid order!</b>\n\n"),
    ("adm_late", "⚠️ دفع متأخر للطلب #{oid} (نفدت الكمية) — تم الاسترجاع تلقائياً.",
     "⚠️ Late payment for order #{oid} (sold out) — refunded automatically."),
    ("adm_manual_refund",
     "🚨 <b>يلزم استرجاع يدوي</b>\nالمستخدم: <code>{uid}</code>\ncharge_id: <code>{charge}</code>\nالخطأ: {err}",
     "🚨 <b>Manual refund needed</b>\nUser: <code>{uid}</code>\ncharge_id: <code>{charge}</code>\nError: {err}"),
    ("ot_head", "📦 <b>طلب #{id}</b> — {status}", "📦 <b>Order #{id}</b> — {status}"),
    ("ot_digital", "💾 منتج رقمي", "💾 Digital product"),

    # ── لوحة المنتجات ──
    ("not_found", "المنتج غير موجود.", "Product not found."),
    ("pt_type_ship", "النوع: 🚚 مادي (شحن + ولايات)", "Type: 🚚 Physical (shipping + wilayas)"),
    ("pt_type_dig", "النوع: 💾 رقمي (بدون شحن)", "Type: 💾 Digital (no shipping)"),
    ("pt_price", "💰 السعر: {p}⭐", "💰 Price: {p}⭐"),
    ("pt_stock", "📦 المخزون: {s}", "📦 Stock: {s}"),
    ("pt_media", "📷 الوسائط (صور/فيديو): {n}", "📷 Media (photos/videos): {n}"),
    ("pt_visible", "الحالة: ✅ ظاهر للزبائن", "Status: ✅ Visible to customers"),
    ("pt_hidden", "الحالة: 🚫 مخفي", "Status: 🚫 Hidden"),
    ("pt_extras", "🧩 حقول الزبون ({n}):", "🧩 Customer fields ({n}):"),
    ("kind_num", "🔢 رقم", "🔢 Number"),
    ("kind_text", "🔤 نص", "🔤 Text"),
    ("pt_deliv", "🎁 محتوى التسليم: نص {a} | ملف {b}", "🎁 Delivery content: text {a} | file {b}"),
    ("b_name", "✏️ الاسم", "✏️ Name"),
    ("b_desc", "📝 الوصف", "📝 Description"),
    ("b_price", "⭐ سعر النجوم", "⭐ Stars price"),
    ("b_stock", "📊 المخزون", "📊 Stock"),
    ("b_photos", "📷 تغيير الصور/الفيديو", "📷 Change photos/videos"),
    ("b_clr", "🧹 مسح الوسائط", "🧹 Clear media"),
    ("b_to_dig", "🔁 تحويل إلى رقمي", "🔁 Switch to digital"),
    ("b_to_phys", "🔁 تحويل إلى مادي (شحن)", "🔁 Switch to physical (shipping)"),
    ("b_deliv", "🎁 محتوى التسليم", "🎁 Delivery content"),
    ("b_extra", "🧩 حقول الزبون ({n})", "🧩 Customer fields ({n})"),
    ("b_hide", "🚫 إخفاء", "🚫 Hide"),
    ("b_show", "✅ إظهار", "✅ Show"),
    ("b_del", "❌ حذف", "❌ Delete"),
    ("b_back", "🔙 المنتجات", "🔙 Products"),
    ("b_add", "➕ إضافة منتج", "➕ Add product"),
    ("products_title", "🗂 <b>المنتجات</b>\nاختر منتجاً لإدارته:", "🗂 <b>Products</b>\nPick a product to manage:"),
    ("del_ask", "⚠️ حذف «{name}» نهائياً؟", "⚠️ Delete “{name}” permanently?"),
    ("b_yes_del", "✅ نعم، احذف", "✅ Yes, delete"),
    ("b_no", "🔙 لا", "🔙 No"),
    ("deleted", "🗑 تم حذف المنتج.", "🗑 Product deleted."),

    # ── إضافة منتج ──
    ("new_title", "➕ <b>منتج جديد</b>\n\nاكتب <b>اسم المنتج</b>:\n(للإلغاء: /cancel)",
     "➕ <b>New product</b>\n\nType the <b>product name</b>:\n(to cancel: /cancel)"),
    ("bad_name", "⚠️ اكتب اسماً من 2 إلى 100 حرف.", "⚠️ Enter a name of 2 to 100 characters."),
    ("ask_price", "💰 اكتب <b>السعر</b> بالنجوم ⭐ (رقم فقط):", "💰 Type the <b>price</b> in stars ⭐ (number only):"),
    ("bad_price", "⚠️ اكتب رقماً صحيحاً أكبر من 0.", "⚠️ Enter a whole number greater than 0."),
    ("b_t_ship", "🚚 مادي — مع شحن (ولايات + عنوان)", "🚚 Physical — with shipping (wilaya + address)"),
    ("b_t_dig", "💾 رقمي — بدون شحن", "💾 Digital — no shipping"),
    ("pick_type", "اختر <b>نوع المنتج</b>:", "Choose the <b>product type</b>:"),
    ("ask_stock_ship", "📦 اكتب <b>الكمية المتوفرة</b> (رقم):", "📦 Type the <b>available quantity</b> (number):"),
    ("ask_stock_dig", "📦 اكتب عدد النسخ المتاحة، أو <b>0</b> لعدد غير محدود:",
     "📦 Type the number of copies available, or <b>0</b> for unlimited:"),
    ("bad_int", "⚠️ اكتب رقماً صحيحاً.", "⚠️ Enter a valid whole number."),
    ("ask_desc", "📝 اكتب <b>وصف المنتج</b> (أو /skip للتخطي):", "📝 Type the <b>product description</b> (or /skip):"),
    ("ask_media",
     "📷 أرسل <b>صور و/أو فيديوهات المنتج</b> (حتى {n} في المجموع، كصور/فيديو وليس كملفات).\nعند الانتهاء اكتب /done — أو /skip إذا لا تريد وسائط.\n(ملاحظة: الوسائط تظهر في البوت فقط، وليس في الموقع)",
     "📷 Send the <b>product photos and/or videos</b> (up to {n} in total, as photos/videos, not files).\nWhen finished send /done — or /skip for no media.\n(Note: media shows in the bot only, not on the website)"),
    ("media_max", "⚠️ الحد الأقصى {n} صور. اكتب /done للمتابعة.", "⚠️ Maximum {n} media items. Send /done to continue."),
    ("what_video", "الفيديو", "video"),
    ("what_photo", "الصورة", "photo"),
    ("media_added", "✅ تمت إضافة {what} ({i}/{n}). أرسل غيرها أو اكتب /done.",
     "✅ Added the {what} ({i}/{n}). Send more or /done."),
    ("media_wrong", "⚠️ أرسل صورة أو فيديو (وليس كملف)، أو /done للمتابعة.",
     "⚠️ Send a photo or video (not as a file), or /done to continue."),
    ("saved_published", "✅ تم حفظ المنتج ونشره.", "✅ Product saved and published."),
    ("ask_delivery",
     "🎁 أرسل <b>ما سيستلمه الزبون بعد الدفع</b>:\n• نص (رابط / كود / تعليمات) — آخر نص يستبدل السابق\n• و/أو ملف (أرسله كـ Document)\n\nعند الانتهاء اكتب /done.",
     "🎁 Send <b>what the customer receives after payment</b>:\n• Text (link / code / instructions) — the latest text replaces the previous one\n• and/or a file (send it as a Document)\n\nWhen finished send /done."),
    ("deliv_text_saved", "✅ تم حفظ النص. أرسل ملفاً أيضاً أو اكتب /done.", "✅ Text saved. Send a file too, or /done."),
    ("deliv_file_saved", "✅ تم حفظ الملف. أرسل نصاً أيضاً أو اكتب /done.", "✅ File saved. Send text too, or /done."),

    # ── التعديل ──
    ("ep_name", "✏️ اكتب الاسم الجديد:", "✏️ Type the new name:"),
    ("ep_desc", "📝 اكتب الوصف الجديد (أو - لمسحه):", "📝 Type the new description (or - to clear it):"),
    ("ep_price", "💰 اكتب السعر الجديد بالنجوم:", "💰 Type the new price in stars:"),
    ("ep_stock", "📦 اكتب المخزون الجديد (للمنتج الرقمي: 0 = غير محدود):",
     "📦 Type the new stock (digital product: 0 = unlimited):"),
    ("photos_replace",
     "📷 أرسل الصور/الفيديوهات الجديدة (حتى {n}) — ستستبدل القديمة.\nعند الانتهاء: /done  |  للإبقاء على القديمة: /skip",
     "📷 Send the new photos/videos (up to {n}) — they will replace the old ones.\nWhen finished: /done  |  to keep the old ones: /skip"),
    ("updated", "✅ تم التحديث.", "✅ Updated."),
    ("adm_cancelled", "تم الإلغاء.", "Cancelled."),

    # ── حقول الزبون (متعددة) ──
    ("x_list_title",
     "🧩 <b>حقول الزبون</b> — «{name}»\n\nالحقل يُحفظ مرة واحدة ويمكنك استعماله في أي منتج بدون إعادة كتابته.\nاختر حقلاً لإدارته أو أضف حقلاً 👇",
     "🧩 <b>Customer fields</b> — “{name}”\n\nA field is saved once and can be reused in any product without retyping it.\nPick a field to manage or add one 👇"),
    ("x_none", "لا توجد حقول في هذا المنتج بعد.", "This product has no fields yet."),
    ("b_x_add", "➕ إضافة حقل", "➕ Add field"),
    ("x_pick_title",
     "➕ اختر <b>حقلاً محفوظاً</b> لإضافته للمنتج «{name}»، أو أنشئ حقلاً جديداً:",
     "➕ Pick a <b>saved field</b> to add to “{name}”, or create a new one:"),
    ("x_pick_none", "لا توجد حقول محفوظة أخرى بعد.", "No other saved fields yet."),
    ("b_x_new", "🆕 إنشاء حقل جديد", "🆕 Create new field"),
    ("b_x_edit", "✏️ تعديل (يسري على كل المنتجات)", "✏️ Edit (applies to all products)"),
    ("b_x_unlink", "➖ إزالة من هذا المنتج", "➖ Remove from this product"),
    ("b_x_kill", "🗑 حذف نهائي من المتجر", "🗑 Delete permanently"),
    ("b_x_back", "🔙 الحقول", "🔙 Fields"),
    ("b_x_prod", "🔙 المنتج", "🔙 Product"),
    ("x_max", "⚠️ الحد الأقصى {n} حقول لكل منتج.", "⚠️ Maximum {n} fields per product."),
    ("x_field_kind", "النوع: {kind}", "Type: {kind}"),
    ("x_field_used", "📦 مستعمل في {n} منتج", "📦 Used in {n} product(s)"),
    ("x_field_norules", "بدون قواعد إضافية", "No extra rules"),
    ("x_ask",
     "🧩 اكتب <b>اسم الحقل / السؤال الذي سيظهر للزبون</b> قبل الدفع.\nمثال: <i>الايدي</i> أو <i>اسم اللاعب</i> أو <i>رقم الحساب</i>\n\n(للإلغاء: /cancel)",
     "🧩 Type the <b>field name / question shown to the customer</b> before payment.\nExample: <i>Player ID</i> or <i>Account number</i>\n\n(to cancel: /cancel)"),
    ("x_removed", "🗑 تم حذف الحقل.", "🗑 Field removed."),
    ("x_bad_len", "⚠️ اكتب نصاً من 2 إلى 200 حرف.", "⚠️ Enter text of 2 to 200 characters."),
    ("b_x_num", "🔢 رقم فقط", "🔢 Numbers only"),
    ("b_x_text", "🔤 نص حر", "🔤 Free text"),
    ("x_type_ask", "ما <b>نوع الإجابة</b> المطلوبة من الزبون؟", "What <b>type of answer</b> do you want from the customer?"),
    ("x_saved", "✅ تم حفظ الحقل. سيُطلب من الزبون قبل الدفع.", "✅ Field saved. It will be requested from the customer before payment."),
    ("x_len_ask",
     "📏 كم يجب أن يكون <b>الطول الإجمالي</b> للإجابة (عدد الخانات)؟\nمثال: <code>20</code>\n(الطول يشمل البداية والنهاية إن حددتهما)\n\nأرسل <b>-</b> إن لم ترد تحديد طول.",
     "📏 What must the <b>total length</b> of the answer be (number of characters)?\nExample: <code>20</code>\n(The length includes the start/end values if you set them)\n\nSend <b>-</b> for no length rule."),
    ("x_len_bad", "⚠️ اكتب رقماً من 1 إلى {max}، أو - للتخطي.", "⚠️ Enter a number from 1 to {max}, or - to skip."),
    ("x_pre_ask",
     "▶️ هل يجب أن <b>تبدأ</b> الإجابة بقيمة معينة؟\nمثال: <code>77</code>\n\nأرسل <b>-</b> للتخطي.",
     "▶️ Must the answer <b>start</b> with a specific value?\nExample: <code>77</code>\n\nSend <b>-</b> to skip."),
    ("x_suf_ask",
     "⏹ هل يجب أن <b>تنتهي</b> الإجابة بقيمة معينة؟\nمثال: <code>99</code>\n\nأرسل <b>-</b> للتخطي.",
     "⏹ Must the answer <b>end</b> with a specific value?\nExample: <code>99</code>\n\nSend <b>-</b> to skip."),
    ("x_digits_only", "⚠️ هذا الحقل رقمي، اكتب أرقاماً فقط (أو - للتخطي).",
     "⚠️ This field is numeric, send digits only (or - to skip)."),
    ("x_rules_long", "⚠️ البداية/النهاية أطول من الطول المحدد ({n}). أعد المحاولة.",
     "⚠️ The start/end values are longer than the set length ({n}). Try again."),

    # ── مكتبة الوسائط + وسائط قائمة المنتجات ──
    ("b_lm", "🖼 وسائط قائمة المنتجات", "🖼 Product list media"),
    ("b_mlib", "📚 مكتبة الوسائط", "📚 Media library"),
    ("b_lib", "📚 إضافة من المكتبة", "📚 Add from library"),
    ("lm_title",
     "🖼 <b>وسائط قائمة المنتجات</b>\nتظهر هذه الصور/الفيديوهات فوق قائمة «كل المنتجات» في البوت.\n\n📷 الحالية: {n}",
     "🖼 <b>Product list media</b>\nThese photos/videos appear above the “All products” list in the bot.\n\n📷 Current: {n}"),
    ("b_lm_up", "📤 رفع جديد", "📤 Upload new"),
    ("b_lm_lib", "📚 من المكتبة", "📚 From library"),
    ("b_lm_clear", "🧹 مسح الوسائط", "🧹 Clear media"),
    ("b_lib_up", "📤 رفع إلى المكتبة", "📤 Upload to library"),
    ("b_done", "✅ تم", "✅ Done"),
    ("lib_title_pick",
     "📚 <b>المكتبة</b> ({total})\nاضغط على رقم الوسيلة لإضافتها (الأرقام مكتوبة على الصور فوق) 👇",
     "📚 <b>Library</b> ({total})\nTap a media number to add it (numbers are written on the previews above) 👇"),
    ("lib_title_manage",
     "📚 <b>مكتبة الوسائط</b> ({total})\nاضغط على 🗑 لحذف وسيلة من المكتبة (لا يحذفها من المنتجات التي تستعملها).",
     "📚 <b>Media library</b> ({total})\nTap 🗑 to delete an item from the library (it stays in products already using it)."),
    ("lib_empty", "المكتبة فارغة. أرسل صوراً أو فيديوهات لتُحفظ فيها.", "The library is empty. Upload photos or videos to save them."),
    ("lib_added", "✅ أُضيفت ({i}/{n})", "✅ Added ({i}/{n})"),
    ("lib_dup", "هذه الوسيلة مضافة مسبقاً.", "This media is already added."),
    ("lib_deleted", "🗑 حُذفت من المكتبة", "🗑 Deleted from library"),
    ("lib_saved", "✅ حُفظت في المكتبة. أرسل غيرها أو اكتب /done.", "✅ Saved to the library. Send more or /done."),
    ("lm_saved", "✅ أُضيفت لقائمة المنتجات وحُفظت في المكتبة ({i}/{n}). أرسل غيرها أو /done.",
     "✅ Added to the product list and saved to the library ({i}/{n}). Send more or /done."),
    ("mup_ask_l",
     "📤 أرسل <b>صور و/أو فيديوهات</b> لقائمة المنتجات (كصور/فيديو وليس كملفات).\nتُحفظ أيضاً في المكتبة لإعادة استعمالها.\nعند الانتهاء اكتب /done.",
     "📤 Send <b>photos and/or videos</b> for the product list (as photos/videos, not files).\nThey are also saved to the library for reuse.\nWhen finished send /done."),
    ("mup_ask_x",
     "📤 أرسل <b>صور و/أو فيديوهات</b> لحفظها في المكتبة (كصور/فيديو وليس كملفات).\nعند الانتهاء اكتب /done.",
     "📤 Send <b>photos and/or videos</b> to save in the library (as photos/videos, not files).\nWhen finished send /done."),

    # ── الموقع (Mini App) ──
    ("btn_open_shop", "🛍 فتح المتجر", "🛍 Open the shop"),
    ("btn_classic", "📋 القائمة العادية", "📋 Classic list"),
    ("open_shop_msg", "👋 أهلاً بك! اضغط الزر لفتح المتجر والشراء بالنجوم أو TON من داخل التطبيق 👇",
     "👋 Welcome! Tap the button to open the shop and pay with Stars or TON right inside the app 👇"),
    ("menu_btn", "🛍 المتجر", "🛍 Shop"),

    # ── الطلبات ──
    ("no_orders", "لا توجد طلبات مدفوعة بعد.", "No paid orders yet."),
    ("shipped_usage", "الاستعمال: <code>/shipped رقم_الطلب</code>", "Usage: <code>/shipped order_number</code>"),
    ("shipped_bad", "الطلب غير موجود أو غير مدفوع.", "Order not found or not paid."),
    ("shipped_ok", "✅ الطلب #{id} أصبح «تم الشحن».", "✅ Order #{id} marked as shipped."),
    ("refund_usage", "الاستعمال: <code>/refund رقم_الطلب</code>", "Usage: <code>/refund order_number</code>"),
    ("refund_bad", "لا يمكن استرجاع هذا الطلب.", "This order cannot be refunded."),
    ("refund_fail", "فشل الاسترجاع: {err}", "Refund failed: {err}"),
    ("refund_ok", "💸 تم استرجاع {amount}⭐ للطلب #{id}.", "💸 Refunded {amount}⭐ for order #{id}."),
    ("help_admin",
     "🛠 <b>أوامر الأدمن</b>\n\n/addproduct — إضافة منتج جديد\n/products — إدارة المنتجات (تعديل، صور، شحن/رقمي، مخزون، حقول الزبون، سعر TON، إخفاء، حذف)\n/orders — الطلبات المدفوعة\n/shipped رقم — تم الشحن\n/refund رقم — استرجاع النجوم\n/lang — تغيير لغة المتجر للجميع\n/diag — فحص الموقع (Mini App)",
     "🛠 <b>Admin commands</b>\n\n/addproduct — add a new product\n/products — manage products (edit, media, physical/digital, stock, customer fields, TON price, hide, delete)\n/orders — paid orders\n/shipped number — mark as shipped\n/refund number — refund the stars\n/lang — change the shop language (for everyone)\n/diag — Mini App health check"),
    ("help_multi",
     "\n\n🤖 <b>البوتات المتعددة</b>\nأرسل هنا <b>توكن بوت جديد</b> (من @BotFather) لربطه وتشغيله فوراً.\n/bots — البوتات المرتبطة\n/delbot ID — فصل بوت وإيقافه",
     "\n\n🤖 <b>Multiple bots</b>\nSend a <b>new bot token</b> (from @BotFather) here to link and start it right away.\n/bots — linked bots\n/delbot ID — unlink and stop a bot"),

    # ── البوتات المتعددة ──
    ("tok_main", "⚠️ هذا توكن البوت الرئيسي نفسه.", "⚠️ This is the main bot's own token."),
    ("tok_invalid", "❌ التوكن غير صالح: {err}", "❌ Invalid token: {err}"),
    ("tok_fail", "❌ تعذّر تشغيل البوت: {err}", "❌ Could not start the bot: {err}"),
    ("tok_ok",
     "✅ تم ربط وتشغيل البوت <b>@{u}</b> (ID: <code>{id}</code>)\n\n🔗 افتحه: https://t.me/{u}\nاضغط /start هناك — أنت أدمنه، ثم استعمل /addproduct لإضافة منتجاتك.\n\nℹ️ منتجاته وطلباته منفصلة تماماً عن هذا البوت.\n🗑 لفصله لاحقاً: <code>/delbot {id}</code>",
     "✅ Bot <b>@{u}</b> (ID: <code>{id}</code>) was linked and started\n\n🔗 Open it: https://t.me/{u}\nPress /start there — you are its admin, then use /addproduct to add your products.\n\nℹ️ Its products and orders are completely separate from this bot.\n🗑 To unlink it later: <code>/delbot {id}</code>"),
    ("bots_none", "لا توجد بوتات مرتبطة. أرسل توكن بوت لإضافته.", "No linked bots. Send a bot token to add one."),
    ("bots_title", "🤖 <b>البوتات المرتبطة</b>\n\n", "🤖 <b>Linked bots</b>\n\n"),
    ("st_run", "🟢 يعمل", "🟢 Running"),
    ("st_stop", "⚪ متوقف", "⚪ Stopped"),
    ("st_off", "🚫 مفصول", "🚫 Unlinked"),
    ("delbot_usage", "الاستعمال: <code>/delbot ID_البوت</code> (انظر /bots)", "Usage: <code>/delbot BOT_ID</code> (see /bots)"),
    ("delbot_nf", "لا يوجد بوت بهذا الـ ID.", "No bot with this ID."),
    ("delbot_ok", "🗑 تم إيقاف وفصل @{u}.\n(بياناته محفوظة؛ إن أرسلت توكنه مجدداً يعود بمنتجاته.)",
     "🗑 @{u} was stopped and unlinked.\n(Its data is kept; if you send its token again it comes back with its products.)"),

    # ── قائمة الأوامر ──
    ("cmd_start", "🏠 الصفحة الرئيسية", "🏠 Home"),
    ("cmd_lang", "🌐 لغة المتجر للجميع", "🌐 Shop language (for everyone)"),
    ("cmd_addproduct", "➕ إضافة منتج", "➕ Add product"),
    ("cmd_products", "🗂 إدارة المنتجات", "🗂 Manage products"),
    ("cmd_orders", "📦 الطلبات المدفوعة", "📦 Paid orders"),
    ("cmd_shipped", "🚚 تم الشحن (رقم)", "🚚 Mark shipped (number)"),
    ("cmd_refund", "💸 استرجاع (رقم)", "💸 Refund (number)"),
    ("cmd_help", "🛠 أوامر الأدمن", "🛠 Admin commands"),
    ("cmd_setwallet", "💎 محفظة TON", "💎 TON wallet"),
    ("cmd_bots", "🤖 البوتات المرتبطة", "🤖 Linked bots"),
    ("cmd_delbot", "🗑 فصل بوت (ID)", "🗑 Unlink a bot (ID)"),
    ("cmd_diag", "🩺 فحص الموقع", "🩺 Mini App check"),

    # ── رسالة بدء التشغيل ──
    ("su_ok", "✅ البوت يعمل.\n", "✅ The bot is running.\n"),
    ("su_pg", "🗄 قاعدة البيانات: PostgreSQL", "🗄 Database: PostgreSQL"),
    ("su_sqlite", "🗄 قاعدة البيانات: SQLite ({path})", "🗄 Database: SQLite ({path})"),
    ("su_warn",
     "\n\n⚠️ الملف مؤقت: عند إعادة النشر قد تضيع المنتجات والبوتات المرتبطة. اربط Volume على /data وضع DB_PATH=/data/shop.db",
     "\n\n⚠️ The file is temporary: products and linked bots may be lost on redeploy. Attach a Volume at /data and set DB_PATH=/data/shop.db"),
    ("su_subs", "\n🤖 بوتات فرعية تعمل: {n}", "\n🤖 Sub-bots running: {n}"),
    ("su_help", "\n\nاكتب /help لعرض الأوامر.", "\n\nSend /help to see the commands."),
]

STR = {"ar": {k: a for k, a, _ in _S}, "en": {k: e for k, _, e in _S}}

PAID_RE = re.compile("(" + "|".join(STR[l]["paid_words"] for l in LANGS) + ")", re.I)

_lang = ContextVar("lang", default=DEFAULT_LANG)


def T(key, lang=None, **kw):
    """نص مترجم حسب لغة المتجر الحالية (أو لغة محددة). لا يرمي خطأً أبداً."""
    lang = lang or _lang.get()
    s = STR.get(lang, STR["ar"]).get(key) or STR["ar"].get(key) or key
    try:
        return s.format(**kw)
    except Exception:
        return s


def wilaya_names(lang=None):
    return WILAYAS.get(lang or _lang.get(), WILAYAS["ar"])


def status_label(status, lang=None):
    key = f"status_{status}"
    return T(key, lang) if key in STR["ar"] else str(status)


# ═════════════════════════════════════════════════════════════════════════════
# 3) سياق المتجر (متعدد البوتات)
# ═════════════════════════════════════════════════════════════════════════════
_shop = ContextVar("shop", default={"id": 0, "admin": ADMIN_ID, "username": None})
RUNNING = {}


def shop_id():
    return _shop.get()["id"]


def shop_admin():
    return _shop.get()["admin"]


# ═════════════════════════════════════════════════════════════════════════════
# 4) قاعدة البيانات
# ═════════════════════════════════════════════════════════════════════════════
USE_PG = False
pool = None
sqlite_conn = None
sqlite_lock = threading.RLock()
PG_MAX_CONN = 40


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


def _pg_get_conn():
    """يأخذ اتصالاً سليماً من الـ pool: ينتظر إن امتلأ، ويستبدل الاتصالات الميتة."""
    deadline = time.time() + 20
    last = None
    while True:
        try:
            conn = pool.getconn()
        except PoolError as e:                     # الـ pool ممتلئ: انتظر قليلاً بدل الفشل
            last = e
            if time.time() > deadline:
                raise
            time.sleep(0.05)
            continue
        if conn.closed:
            try:
                pool.putconn(conn, close=True)
            except Exception:
                pass
            if time.time() > deadline:
                raise last or RuntimeError("no healthy connection")
            continue
        return conn


@contextmanager
def cursor():
    if USE_PG:
        conn = _pg_get_conn()
        broken = False
        try:
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    yield Cur(c, True)
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            broken = True
            raise
        finally:
            try:
                pool.putconn(conn, close=bool(conn.closed) or broken)
            except Exception:
                pass
    else:
        with sqlite_lock:
            with sqlite_conn:
                c = sqlite_conn.cursor()
                try:
                    yield Cur(c, False)
                finally:
                    c.close()


def ensure_column(table, col, typ):
    try:
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
    except Exception as e:
        log.warning("ensure_column %s.%s failed: %s", table, col, str(e)[:120])


def init_db():
    global USE_PG, pool, sqlite_conn
    urls = [os.environ.get("DATABASE_URL"), os.environ.get("DATABASE_PUBLIC_URL")]
    for u in [x for x in urls if x]:
        try:
            pool = ThreadedConnectionPool(
                2, PG_MAX_CONN, u, connect_timeout=8,
                keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
            )
            USE_PG = True
            log.info("Connected to PostgreSQL")
            break
        except Exception as e:
            log.warning("PostgreSQL connection failed: %s", str(e).strip()[:150])

    if not USE_PG:
        sqlite_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        sqlite_conn.row_factory = sqlite3.Row
        try:
            sqlite_conn.execute("PRAGMA journal_mode=WAL")
            sqlite_conn.execute("PRAGMA synchronous=NORMAL")
        except Exception as e:
            log.warning("sqlite pragma failed: %s", e)
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
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS shop_fields (
                id {pk},
                shop_id BIGINT NOT NULL DEFAULT 0,
                label TEXT NOT NULL,
                kind TEXT DEFAULT 'text',
                flen INT DEFAULT 0,
                prefix TEXT,
                suffix TEXT
            )
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS shop_product_fields (
                id {pk},
                product_id INT NOT NULL,
                field_id INT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shop_users (
                shop_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                phone TEXT,
                PRIMARY KEY (shop_id, user_id)
            )
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS shop_media (
                id {pk},
                shop_id BIGINT NOT NULL DEFAULT 0,
                file_id TEXT NOT NULL,
                unique_id TEXT,
                kind TEXT DEFAULT 'photo',
                created_at BIGINT
            )
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS shop_extras (
                id {pk},
                product_id INT NOT NULL,
                label TEXT NOT NULL,
                kind TEXT DEFAULT 'text',
                flen INT DEFAULT 0,
                prefix TEXT,
                suffix TEXT
            )
        """)

    ensure_column("shop_orders", "product_id", "INT")
    ensure_column("shop_orders", "product_name", "TEXT")
    ensure_column("shop_orders", "reserved_until", "BIGINT")
    ensure_column("shop_photos", "kind", "TEXT DEFAULT 'photo'")
    ensure_column("shop_products", "shop_id", "BIGINT DEFAULT 0")
    ensure_column("shop_orders", "shop_id", "BIGINT DEFAULT 0")
    ensure_column("shop_products", "extra_label", "TEXT")
    ensure_column("shop_products", "extra_type", "TEXT")
    ensure_column("shop_orders", "extra_label", "TEXT")
    ensure_column("shop_orders", "extra_value", "TEXT")
    ensure_column("shop_products", "extra_len", "INT DEFAULT 0")
    ensure_column("shop_products", "extra_prefix", "TEXT")
    ensure_column("shop_products", "extra_suffix", "TEXT")
    ensure_column("shop_orders", "extras_json", "TEXT")
    ensure_column("shop_products", "price_ton", "DOUBLE PRECISION")
    ensure_column("shop_orders", "ton_nano", "BIGINT")
    ensure_column("shop_products", "stars_on", "INT DEFAULT 1")

    if USE_PG:
        try:
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
        except Exception as e:
            log.warning("timestamp migration failed: %s", str(e)[:150])

    for ddl in (
        "CREATE INDEX IF NOT EXISTS ix_prod_shop ON shop_products (shop_id, active)",
        "CREATE INDEX IF NOT EXISTS ix_photo_prod ON shop_photos (product_id)",
        "CREATE INDEX IF NOT EXISTS ix_ord_prod ON shop_orders (product_id, status, reserved_until)",
        "CREATE INDEX IF NOT EXISTS ix_ord_shop ON shop_orders (shop_id, status)",
        "CREATE INDEX IF NOT EXISTS ix_ord_user ON shop_orders (user_id, status)",
        "CREATE INDEX IF NOT EXISTS ix_pf_prod ON shop_product_fields (product_id)",
        "CREATE INDEX IF NOT EXISTS ix_pf_field ON shop_product_fields (field_id)",
        "CREATE INDEX IF NOT EXISTS ix_fields_shop ON shop_fields (shop_id)",
        "CREATE INDEX IF NOT EXISTS ix_media_shop ON shop_media (shop_id, unique_id)",
    ):
        try:
            with cursor() as cur:
                cur.execute(ddl)
        except Exception as e:
            log.warning("index failed: %s", str(e)[:120])

    migrate_legacy_extras()
    log.info("DB ready (%s)", "PostgreSQL" if USE_PG else "SQLite")


def migrate_legacy_extras():
    """ينقل الحقول القديمة إلى المكتبة الجديدة مرة واحدة."""
    try:
        if _load_setting("extras_v2"):
            return
        cache, n = {}, 0

        def field_for(cur, sid, label, kind, flen, prefix, suffix):
            key = (sid, label, kind, flen or 0, prefix, suffix)
            if key not in cache:
                cache[key] = _insert_field(cur, sid, label, kind, flen, prefix, suffix)
            return cache[key]

        with cursor() as cur:
            cur.execute("SELECT e.*, p.shop_id AS sid FROM shop_extras e "
                        "JOIN shop_products p ON p.id = e.product_id ORDER BY e.id")
            rows = cur.fetchall()
            for r in rows:
                fid = field_for(cur, r.get("sid") or 0, r["label"], r.get("kind") or "text",
                                r.get("flen"), r.get("prefix"), r.get("suffix"))
                cur.execute("INSERT INTO shop_product_fields (product_id, field_id) VALUES (?,?)",
                            (r["product_id"], fid))
                n += 1
            cur.execute("SELECT * FROM shop_products WHERE extra_label IS NOT NULL AND extra_label <> ''")
            olds = cur.fetchall()
            for p in olds:
                fid = field_for(cur, p.get("shop_id") or 0, p["extra_label"], p.get("extra_type") or "text",
                                p.get("extra_len"), p.get("extra_prefix"), p.get("extra_suffix"))
                cur.execute("INSERT INTO shop_product_fields (product_id, field_id) VALUES (?,?)",
                            (p["id"], fid))
                cur.execute("UPDATE shop_products SET extra_label=NULL, extra_type=NULL, extra_len=0, "
                            "extra_prefix=NULL, extra_suffix=NULL WHERE id=?", (p["id"],))
                n += 1
        _save_setting("extras_v2", "1")
        if n:
            log.info("Migrated %d customer fields to the library", n)
    except Exception as e:
        log.warning("extras migration failed: %s", e)


# ── الإعدادات: لغة المتجر ──
_SHOP_LANG = {}


def _save_setting(key, value):
    with cursor() as cur:
        cur.execute(
            "INSERT INTO shop_settings (key, value) VALUES (?,?) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


def _load_setting(key):
    with cursor() as cur:
        cur.execute("SELECT value FROM shop_settings WHERE key=?", (key,))
        r = cur.fetchone()
    return r["value"] if r and r["value"] else None


def get_shop_lang(sid):
    if sid in _SHOP_LANG:
        return _SHOP_LANG[sid]
    lang = DEFAULT_LANG
    try:
        v = _load_setting(f"lang_{sid}")
        if v in LANGS:
            lang = v
        _SHOP_LANG[sid] = lang
    except Exception as e:
        log.warning("get_shop_lang failed: %s", e)
    return lang


def set_shop_lang(sid, lang):
    _save_setting(f"lang_{sid}", lang)
    _SHOP_LANG[sid] = lang


# ── محفظة TON ──
_WALLET_CACHE = {}


def get_wallet():
    """محفظة المتجر الحالي، وإلا محفظة البوت الرئيسي."""
    sid = shop_id()
    hit = _WALLET_CACHE.get(sid)
    now = time.time()
    if hit and hit[0] > now:
        return hit[1]
    val = None
    for s in (sid, 0):
        try:
            v = _load_setting(f"ton_wallet_{s}")
            if v:
                val = v
                break
        except Exception as e:
            log.warning("get_wallet failed: %s", e)
    val = val or TON_WALLET
    _WALLET_CACHE[sid] = (now + 15, val)
    return val


def set_wallet(w):
    _save_setting(f"ton_wallet_{shop_id()}", w)
    _WALLET_CACHE.clear()


# ── المنتجات ──
PRODUCT_FIELDS = {"name", "description", "price", "stock", "shipping", "delivery_text", "delivery_file", "active",
                  "price_ton", "stars_on"}


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
        cur.execute(
            "DELETE FROM shop_product_fields WHERE product_id IN (SELECT id FROM shop_products WHERE id=? AND shop_id=?)",
            (pid, shop_id()),
        )
        cur.execute("DELETE FROM shop_products WHERE id=? AND shop_id=?", (pid, shop_id()))


def get_media(pid):
    with cursor() as cur:
        cur.execute("SELECT file_id, kind FROM shop_photos WHERE product_id=? ORDER BY id", (pid,))
        return [{"file_id": r["file_id"], "kind": r["kind"] or "photo"} for r in cur.fetchall()]


def count_media(pid):
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM shop_photos WHERE product_id=?", (pid,))
        return cur.fetchone()["n"]


def add_photo(pid, file_id, kind="photo"):
    with cursor() as cur:
        cur.execute("INSERT INTO shop_photos (product_id, file_id, kind) VALUES (?,?,?)", (pid, file_id, kind))


def clear_photos(pid):
    with cursor() as cur:
        cur.execute("DELETE FROM shop_photos WHERE product_id=?", (pid,))


# ── حقول الزبون ──
def _insert_field(cur, sid, label, kind, flen, prefix, suffix):
    cur.execute("""
        INSERT INTO shop_fields (shop_id, label, kind, flen, prefix, suffix)
        VALUES (?,?,?,?,?,?) RETURNING id
    """, (sid, label, kind, flen or 0, prefix, suffix))
    return cur.fetchone()["id"]


def list_extras(pid):
    with cursor() as cur:
        cur.execute("""
            SELECT f.* FROM shop_fields f
            JOIN shop_product_fields l ON l.field_id = f.id
            WHERE l.product_id=? AND f.shop_id=? ORDER BY l.id
        """, (pid, shop_id()))
        return cur.fetchall()


def list_library():
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_fields WHERE shop_id=? ORDER BY id", (shop_id(),))
        return cur.fetchall()


def get_extra(fid):
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_fields WHERE id=? AND shop_id=?", (fid, shop_id()))
        return cur.fetchone()


def add_extra(label, kind, flen, prefix, suffix):
    with cursor() as cur:
        return _insert_field(cur, shop_id(), label, kind, flen, prefix, suffix)


def update_extra(fid, label, kind, flen, prefix, suffix):
    with cursor() as cur:
        cur.execute("""
            UPDATE shop_fields SET label=?, kind=?, flen=?, prefix=?, suffix=?
            WHERE id=? AND shop_id=?
        """, (label, kind, flen or 0, prefix, suffix, fid, shop_id()))


def attach_extra(pid, fid):
    if not get_product(pid) or not get_extra(fid):
        return False
    with cursor() as cur:
        cur.execute("SELECT id FROM shop_product_fields WHERE product_id=? AND field_id=?", (pid, fid))
        if not cur.fetchone():
            cur.execute("INSERT INTO shop_product_fields (product_id, field_id) VALUES (?,?)", (pid, fid))
    return True


def detach_extra(pid, fid):
    if not get_product(pid):
        return
    with cursor() as cur:
        cur.execute("DELETE FROM shop_product_fields WHERE product_id=? AND field_id=?", (pid, fid))


def delete_extra(fid):
    if not get_extra(fid):
        return
    with cursor() as cur:
        cur.execute("DELETE FROM shop_product_fields WHERE field_id=?", (fid,))
        cur.execute("DELETE FROM shop_fields WHERE id=? AND shop_id=?", (fid, shop_id()))


def field_usage(fid):
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM shop_product_fields WHERE field_id=?", (fid,))
        return cur.fetchone()["n"]


# ── هاتف الزبون ──
def save_phone(uid, phone):
    with cursor() as cur:
        cur.execute(
            "INSERT INTO shop_users (shop_id, user_id, phone) VALUES (?,?,?) "
            "ON CONFLICT (shop_id, user_id) DO UPDATE SET phone=excluded.phone",
            (shop_id(), uid, phone),
        )


def get_phone(uid):
    with cursor() as cur:
        cur.execute("SELECT phone FROM shop_users WHERE shop_id=? AND user_id=?", (shop_id(), uid))
        r = cur.fetchone()
    return r["phone"] if r and r["phone"] else None


# ── مكتبة الوسائط (للبوت فقط) ──
def lib_add(file_id, unique_id, kind="photo"):
    with cursor() as cur:
        if unique_id:
            cur.execute("SELECT id FROM shop_media WHERE shop_id=? AND unique_id=?", (shop_id(), unique_id))
            r = cur.fetchone()
            if r:
                return r["id"]
        cur.execute("""
            INSERT INTO shop_media (shop_id, file_id, unique_id, kind, created_at)
            VALUES (?,?,?,?,?) RETURNING id
        """, (shop_id(), file_id, unique_id, kind, int(time.time())))
        return cur.fetchone()["id"]


def lib_list(limit, offset):
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_media WHERE shop_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
                    (shop_id(), limit, offset))
        return cur.fetchall()


def lib_count():
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM shop_media WHERE shop_id=?", (shop_id(),))
        return cur.fetchone()["n"]


def lib_get(mid):
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_media WHERE id=? AND shop_id=?", (mid, shop_id()))
        return cur.fetchone()


def lib_delete(mid):
    with cursor() as cur:
        cur.execute("DELETE FROM shop_media WHERE id=? AND shop_id=?", (mid, shop_id()))


def get_list_media():
    try:
        v = _load_setting(f"list_media_{shop_id()}")
        return json.loads(v) if v else []
    except Exception as e:
        log.warning("get_list_media failed: %s", e)
        return []


def set_list_media(items):
    _save_setting(f"list_media_{shop_id()}", json.dumps(items))


# ── المخزون والحجز ──
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
    p = get_product(pid)
    if not p or not p["active"] or p["stock"] == 0:
        return "sold"
    if p["stock"] < 0:
        return "available"
    with cursor() as cur:
        held = _held_by_others(cur, pid, uid)
    return "available" if p["stock"] - held > 0 else "held"


# ── الطلبات ──
def place_order(user, pid, phone, wilaya, address, extras=None):
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
        extras_json = json.dumps(extras, ensure_ascii=False) if extras else None
        cur.execute("""
            INSERT INTO shop_orders
              (user_id, username, full_name, phone, wilaya, address, amount, created_at,
               product_id, product_name, reserved_until, shop_id, extras_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id
        """, (user.id, user.username or "", user.full_name or "", phone, wilaya, address,
              p["price"], now, pid, p["name"], now + RESERVE_MINUTES * 60, shop_id(), extras_json))
        return cur.fetchone()["id"], "ok"


def hold_for_payment(order_id, user_id):
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


# ═════════════════════════════════════════════════════════════════════════════
# 5) دوال مساعدة للنصوص والأزرار
# ═════════════════════════════════════════════════════════════════════════════
def esc(s):
    return html.escape(str(s or ""))


def stock_label(stock):
    return T("unlimited") if stock < 0 else str(stock)


def fmt_ton(v):
    try:
        return f"{float(v):.9f}".rstrip("0").rstrip(".") or "0"
    except (TypeError, ValueError):
        return "0"


def parse_ton_price(text):
    try:
        v = float(text.strip().translate(AR_DIGITS).replace(",", ".").replace(" ", ""))
    except ValueError:
        return None
    return v if 0 < v <= 1000000 else None


def stars_on(p):
    return p.get("stars_on") != 0


def ton_on(p):
    return bool(p.get("price_ton")) and bool(get_wallet())


def pay_methods(p):
    m = []
    if stars_on(p) and (p.get("price") or 0) > 0:
        m.append("stars")
    if ton_on(p):
        m.append("ton")
    return m


def price_label(p):
    parts = []
    m = pay_methods(p)
    if "stars" in m:
        parts.append(f"{p['price']}⭐")
    if "ton" in m:
        parts.append(f"{fmt_ton(p['price_ton'])} TON")
    return " / ".join(parts) or "—"


def extra_rules(f):
    r = []
    n = f.get("flen") or 0
    if n:
        r.append(T("rule_len_num" if f.get("kind") == "number" else "rule_len_text", n=n))
    if f.get("prefix"):
        r.append(T("rule_pre", v=esc(f["prefix"])))
    if f.get("suffix"):
        r.append(T("rule_suf", v=esc(f["suffix"])))
    return r


def kind_label(f):
    return T("kind_num") if f.get("kind") == "number" else T("kind_text")


def check_extra(f, text):
    t = (text or "").strip()
    if f.get("kind") == "number":
        t = t.translate(AR_DIGITS).replace(" ", "")
        if not re.fullmatch(r"\d{1,%d}" % MAX_EXTRA_LEN, t):
            return None, T("extra_bad_num")
    elif not (1 <= len(t) <= 300):
        return None, T("extra_bad_text")

    n = f.get("flen") or 0
    if n and len(t) != n:
        return None, T("extra_bad_len", n=n, got=len(t))
    pre, suf = f.get("prefix"), f.get("suffix")
    if pre and not t.startswith(pre):
        return None, T("extra_bad_pre", v=esc(pre))
    if suf and not t.endswith(suf):
        return None, T("extra_bad_suf", v=esc(suf))
    return t, None


def order_extras(o):
    items = []
    if o.get("extras_json"):
        try:
            items = json.loads(o["extras_json"]) or []
        except Exception:
            items = []
    if not items and o.get("extra_value"):
        items = [{"label": o.get("extra_label") or "—", "value": o["extra_value"]}]
    return items


def order_text(o, lang=None):
    name = o.get("product_name") or T("unknown_product", lang)
    uname = f"@{esc(o['username'])}" if o.get("username") else "—"
    t = (
        T("ot_head", lang, id=o["id"], status=esc(status_label(o["status"], lang))) + "\n"
        f"🛍 {esc(name)}\n"
        f"👤 {esc(o['full_name'])} ({uname}) — ID: <code>{o['user_id']}</code>\n"
    )
    if o.get("wilaya"):
        t += f"📱 +{esc(o['phone'])}\n📍 {esc(o['wilaya'])}\n🏠 {esc(o['address'])}\n"
    else:
        t += T("ot_digital", lang) + "\n"
    for x in order_extras(o):
        t += f"🧩 {esc(x.get('label') or '—')}: <code>{esc(x.get('value'))}</code>\n"
    if str(o.get("charge_id") or "").startswith("ton_"):
        t += f"💎 {fmt_ton((o.get('ton_nano') or 0) / 1e9)} TON\n"
    else:
        t += f"💰 {o['amount']}⭐"
    return t


def wilaya_keyboard():
    rows, row = [], []
    for i, name in enumerate(wilaya_names(), start=1):
        row.append(InlineKeyboardButton(f"{i:02d} {name}", callback_data=f"w_{i}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(T("btn_cancel"), callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def lang_keyboard():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(T("lang_btn_ar"), callback_data="setlang_ar"),
        InlineKeyboardButton(T("lang_btn_en"), callback_data="setlang_en"),
    ]])


def is_admin(update):
    a = shop_admin()
    return bool(update.effective_user) and a != 0 and update.effective_user.id == a


# ═════════════════════════════════════════════════════════════════════════════
# 6) واجهة الزبون: العرض (داخل البوت)
# ═════════════════════════════════════════════════════════════════════════════
async def send_with_media(message, text, markup, media):
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


async def show_list(message, prods):
    rows = []
    for p in prods:
        mark = "❌ " if p["stock"] == 0 else ("🛠 " if p["shipping"] else "💾 ")
        rows.append([InlineKeyboardButton(f"{mark}{p['name'][:40]} — {price_label(p)}", callback_data=f"prod_{p['id']}")])
    await send_with_media(message, T("list_title"), InlineKeyboardMarkup(rows), get_list_media()[:MAX_PHOTOS])


async def show_product(message, p, uid):
    state = availability(p["id"], uid)
    text = f"{'🛠' if p['shipping'] else '💾'} <b>{esc(p['name'])}</b>\n\n"
    if p["description"]:
        text += f"{esc(p['description'])}\n\n"
    methods = pay_methods(p)
    if "stars" in methods:
        text += T("price_line", price=p["price"])
    if "ton" in methods:
        text += T("price_ton_line", ton=fmt_ton(p["price_ton"]))
    if p["shipping"]:
        if p["stock"] >= 0:
            text += T("stock_line", n=p["stock"])
        text += T("dz_only")
    else:
        text += T("digital_note")
    text += "\n"

    rows = []
    if state == "available" and not methods:
        text += T("no_pay_method")
    elif state == "available":
        text += T("buy_prompt")
        rows.append([InlineKeyboardButton(T("btn_buy"), callback_data=f"buy_{p['id']}")])
    else:
        text += T("unavail_" + state)
    if len(list_products()) > 1:
        rows.append([InlineKeyboardButton(T("btn_all_products"), callback_data="list")])
    markup = InlineKeyboardMarkup(rows) if rows else None

    await send_with_media(message, text, markup, get_media(p["id"])[:MAX_PHOTOS])


async def send_home(message, uid):
    prods = list_products()
    if not prods:
        msg = T("no_products")
        if shop_admin() and uid == shop_admin():
            msg += T("admin_add_hint")
        await message.reply_text(msg)
        return
    if WEBAPP_URL:
        try:
            await message.reply_text(T("open_shop_msg"), reply_markup=webapp_keyboard())
            return
        except Exception as e:
            log.error("webapp button failed (%s) -> classic list", e)
    if len(prods) == 1:
        await show_product(message, prods[0], uid)
    else:
        await show_list(message, prods)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update):
        await set_admin_commands(context.bot, shop_admin(), shop_id())
    await send_home(update.message, update.effective_user.id)
    return ConversationHandler.END


async def prod_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    p = get_product(int(q.data.split("_")[1]))
    if not p or not p["active"]:
        await q.message.reply_text(T("product_gone"))
        return
    await show_product(q.message, p, q.from_user.id)


async def list_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    prods = list_products()
    if prods:
        await show_list(q.message, prods)


# ── اللغة ──
async def cmd_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    await update.message.reply_text(T("lang_pick"), reply_markup=lang_keyboard())


async def lang_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        return
    lang = q.data.split("_")[1]
    if lang not in LANGS:
        return
    set_shop_lang(shop_id(), lang)
    _lang.set(lang)
    await setup_commands(context.bot, shop_admin(), shop_id())
    try:
        await q.message.edit_text(T("lang_set"))
    except Exception:
        await q.message.reply_text(T("lang_set"))
    await send_home(q.message, q.from_user.id)


# ═════════════════════════════════════════════════════════════════════════════
# 7) واجهة الزبون: خطوات الطلب
# ═════════════════════════════════════════════════════════════════════════════
async def send_summary(message, context):
    d = context.user_data
    p = get_product(d["pid"])
    methods = pay_methods(p)
    rows = []
    if "stars" in methods:
        rows.append([InlineKeyboardButton(T("btn_pay", price=p["price"]), callback_data="pay")])
    if "ton" in methods:
        rows.append([InlineKeyboardButton(T("btn_pay_ton", ton=f"{fmt_ton(p['price_ton'])} TON"),
                                          callback_data="payton")])
    if p["shipping"]:
        rows.append([InlineKeyboardButton(T("btn_edit_addr"), callback_data="restart")])
    rows.append([InlineKeyboardButton(T("btn_cancel"), callback_data="cancel")])
    text = T("summary_title", name=esc(p["name"]))
    if p["shipping"]:
        text += f"📱 +{esc(d['phone'])}\n📍 {esc(d['wilaya'])}\n🏠 {esc(d['address'])}\n"
    else:
        text += T("digital_note")
    for f, v in zip(list_extras(p["id"]), d.get("xvals", [])):
        text += f"🧩 {esc(f['label'])}: <b>{esc(v)}</b>\n"
    text += T("summary_amount", amount=price_label(p))
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(rows))


async def ask_extra_or_summary(message, context, p):
    extras = list_extras(p["id"])
    done = context.user_data.setdefault("xvals", [])
    i = len(done)
    if i < len(extras):
        f = extras[i]
        hint = T("extra_hint_num") if f.get("kind") == "number" else T("extra_hint_text")
        head = f"🧩 ({i + 1}/{len(extras)}) " if len(extras) > 1 else "🧩 "
        text = f"{head}<b>{esc(f['label'])}</b>\n{hint}\n"
        rules = extra_rules(f)
        if rules:
            text += "\n" + "\n".join(rules) + "\n"
        text += f"\n{T('cancel_hint')}"
        await message.reply_text(text)
        return EXTRA
    await send_summary(message, context)
    return CONFIRM


async def got_extra(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = context.user_data
    p = get_product(d["pid"]) if d.get("pid") else None
    if not p:
        await update.message.reply_text(T("session_expired"))
        return ConversationHandler.END
    extras = list_extras(p["id"])
    done = d.setdefault("xvals", [])
    if len(done) >= len(extras):
        await send_summary(update.message, context)
        return CONFIRM

    f = extras[len(done)]
    val, err = check_extra(f, update.message.text)
    if err:
        await update.message.reply_text(err)
        return EXTRA
    done.append(val)
    return await ask_extra_or_summary(update.message, context, p)


async def buy_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pid = int(q.data.split("_")[1])
    state = availability(pid, q.from_user.id)
    if state != "available":
        await q.message.reply_text(T("unavail_" + state))
        return ConversationHandler.END
    p = get_product(pid)
    if not pay_methods(p):
        await q.message.reply_text(T("no_pay_method"))
        return ConversationHandler.END
    context.user_data.clear()
    context.user_data["pid"] = pid
    context.user_data["xvals"] = []

    if not p["shipping"]:
        return await ask_extra_or_summary(q.message, context, p)

    kb = ReplyKeyboardMarkup(
        [[KeyboardButton(T("btn_share_phone"), request_contact=True)]],
        resize_keyboard=True, one_time_keyboard=True,
    )
    await q.message.reply_text(T("phone_ask"), reply_markup=kb)
    return PHONE


async def got_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    c = update.message.contact
    if c.user_id != update.effective_user.id:
        await update.message.reply_text(T("phone_not_yours"))
        return PHONE
    phone = "".join(ch for ch in c.phone_number if ch.isdigit())
    if not (phone.startswith("213") and len(phone) == 12):
        await update.message.reply_text(T("phone_not_dz"), reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    context.user_data["phone"] = phone
    save_phone(update.effective_user.id, phone)
    await update.message.reply_text(T("phone_ok"), reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text(T("pick_wilaya"), reply_markup=wilaya_keyboard())
    return WILAYA


async def phone_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(T("phone_hint"))
    return PHONE


async def got_wilaya(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data.split("_")[1])
    context.user_data["wilaya"] = f"{idx:02d} - {WILAYAS['ar'][idx - 1]} / {WILAYAS['en'][idx - 1]}"
    await q.message.edit_text(T("wilaya_chosen", w=esc(wilaya_names()[idx - 1])))
    return ADDRESS


async def got_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    addr = update.message.text.strip()
    if len(addr) < 10 or len(addr) > 300:
        await update.message.reply_text(T("addr_bad"))
        return ADDRESS
    context.user_data["address"] = addr
    p = get_product(context.user_data.get("pid") or 0)
    if not p:
        await update.message.reply_text(T("session_expired"))
        return ConversationHandler.END
    return await ask_extra_or_summary(update.message, context, p)


async def restart_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.message.reply_text(T("pick_wilaya"), reply_markup=wilaya_keyboard())
    return WILAYA


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(T("cancelled"), reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text(T("cancelled"), reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


# ═════════════════════════════════════════════════════════════════════════════
# 8) الدفع (نجوم + TON)
# ═════════════════════════════════════════════════════════════════════════════
TON_TASKS = set()


def ton_memo(oid):
    return f"order_{oid}"


def to_nano(ton):
    return int(round(float(ton) * 1_000_000_000))


def parse_order_id(payload):
    try:
        return int(payload.split("_")[1])
    except Exception:
        return None


async def _pay(update: Update, context: ContextTypes.DEFAULT_TYPE, method):
    q = update.callback_query
    await q.answer()
    d = context.user_data
    pid = d.get("pid")
    p = get_product(pid) if pid else None
    if not p:
        await q.message.reply_text(T("session_expired"))
        return ConversationHandler.END
    if method not in pay_methods(p):
        await q.message.reply_text(T("session_expired"))
        return ConversationHandler.END
    if p["shipping"] and not all(k in d for k in ("phone", "wilaya", "address")):
        await q.message.reply_text(T("session_expired"))
        return ConversationHandler.END

    extras = list_extras(pid)
    xvals = d.get("xvals", [])
    if len(xvals) != len(extras):
        await q.message.reply_text(T("session_expired"))
        return ConversationHandler.END
    pairs = [{"label": f["label"], "value": v} for f, v in zip(extras, xvals)]

    oid, why = place_order(q.from_user, pid, d.get("phone"), d.get("wilaya"), d.get("address"), pairs)
    if not oid:
        await q.message.reply_text(T("unavail_" + (why if why in ("sold", "held") else "sold")))
        return ConversationHandler.END
    order = get_order(oid)

    if method == "ton":
        wallet = get_wallet()
        if not (wallet and p.get("price_ton")):
            set_order(oid, "cancelled")
            await q.message.reply_text(T("session_expired"))
            return ConversationHandler.END
        nano = to_nano(p["price_ton"])
        with cursor() as cur:
            cur.execute("UPDATE shop_orders SET ton_nano=? WHERE id=?", (nano, oid))
        await q.message.reply_text(T("reserved_ton", m=RESERVE_MINUTES))
        memo = ton_memo(oid)
        url = f"https://app.tonkeeper.com/transfer/{wallet}?amount={nano}&text={memo}"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(T("btn_ton_open"), url=url)],
            [InlineKeyboardButton(T("btn_ton_check"), callback_data=f"tonchk_{oid}")],
        ])
        await context.bot.send_message(
            q.message.chat_id,
            T("ton_pay_msg", amt=fmt_ton(nano / 1e9), addr=esc(wallet), memo=esc(memo)),
            reply_markup=kb,
        )
        task = asyncio.create_task(watch_ton(context, oid))
        TON_TASKS.add(task)
        task.add_done_callback(TON_TASKS.discard)
        return ConversationHandler.END

    await q.message.reply_text(T("reserved", m=RESERVE_MINUTES))
    desc = T("invoice_ship_desc", name=p["name"]) if p["shipping"] else (p["description"] or p["name"])
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


async def pay_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _pay(update, context, "stars")


async def payton_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _pay(update, context, "ton")


async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.pre_checkout_query
    oid = parse_order_id(q.invoice_payload)
    o = hold_for_payment(oid, q.from_user.id) if (oid is not None and q.currency == "XTR") else None
    if o is not None and q.total_amount == o["amount"]:
        await q.answer(ok=True)
    else:
        await q.answer(ok=False, error_message=T("precheck_fail"))


async def notify_admin(context, build):
    a = shop_admin()
    if a:
        try:
            text = build(get_shop_lang(shop_id()))
            await context.bot.send_message(a, text)
        except Exception as e:
            log.error("admin notify failed: %s", e)


async def deliver_digital(context, chat_id, p):
    sent = False
    if p.get("delivery_text"):
        await context.bot.send_message(chat_id, T("your_product", text=esc(p["delivery_text"])))
        sent = True
    if p.get("delivery_file"):
        await context.bot.send_document(chat_id, p["delivery_file"])
        sent = True
    return sent


async def finalize_paid(context, uid, oid, status, order, charge, stars=True):
    if status == "ok":
        p = get_product(order["product_id"]) if order.get("product_id") else None
        if p and not p["shipping"]:
            await context.bot.send_message(uid, T("paid_ok_digital", oid=oid))
            ok = False
            try:
                ok = await deliver_digital(context, uid, p)
            except Exception as e:
                log.error("digital delivery failed: %s", e)
            if not ok:
                await context.bot.send_message(uid, T("will_send"))
                await notify_admin(context, lambda L: T("adm_digital_nodeliv", L, oid=oid, uid=uid))
        else:
            await context.bot.send_message(uid, T("paid_ok_ship", oid=oid))
        await notify_admin(context, lambda L: T("adm_new_paid", L) + order_text(order, L))
        return
    if status == "dup":
        return

    if stars:
        try:
            await context.bot.refund_star_payment(user_id=uid, telegram_payment_charge_id=charge)
            if oid:
                set_order(oid, "refunded", charge)
            await context.bot.send_message(uid, T("refunded_soldout"))
            await notify_admin(context, lambda L: T("adm_late", L, oid=oid))
        except Exception as e:
            log.error("auto refund failed: %s", e)
            await context.bot.send_message(uid, T("refund_problem"))
            await notify_admin(
                context,
                lambda L: T("adm_manual_refund", L, uid=uid, charge=esc(charge), err=esc(e)),
            )
    else:
        try:
            await context.bot.send_message(uid, T("ton_manual_user"))
        except Exception as e:
            log.warning("ton user notify failed: %s", e)
        await notify_admin(
            context,
            lambda L: T("adm_ton_manual", L, oid=oid, uid=uid, h=esc(charge), why=esc(status)),
        )


async def on_paid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sp = update.message.successful_payment
    uid = update.effective_user.id
    oid = parse_order_id(sp.invoice_payload)
    charge = sp.telegram_payment_charge_id
    status, order = mark_paid(oid, charge) if oid else ("invalid", None)
    await finalize_paid(context, uid, oid, status, order, charge, stars=True)


# ── TON: التحقق من التحويلات ──
_TON_HTTP = None


def ton_http():
    global _TON_HTTP
    if _TON_HTTP is None or _TON_HTTP.is_closed:
        _TON_HTTP = httpx.AsyncClient(timeout=15)
    return _TON_HTTP


async def find_ton_payment(wallet, oid, min_nano, since):
    if not wallet or not min_nano:
        return None
    params = {"address": wallet, "limit": 50}
    if TONCENTER_KEY:
        params["api_key"] = TONCENTER_KEY
    try:
        r = await ton_http().get("https://toncenter.com/api/v2/getTransactions", params=params)
        data = r.json()
    except Exception as e:
        log.warning("toncenter request failed: %s", e)
        return None
    if not data.get("ok"):
        log.warning("toncenter error: %s", str(data)[:150])
        return None
    memo = ton_memo(oid)
    for tx in data.get("result", []):
        m = tx.get("in_msg") or {}
        if not m.get("source"):
            continue
        if int(tx.get("utime") or 0) < int(since or 0) - 60:
            continue
        if (m.get("message") or "").strip() != memo:
            continue
        try:
            value = int(m.get("value") or 0)
        except ValueError:
            continue
        if value >= int(min_nano):
            return tx["transaction_id"]["hash"]
    return None


async def settle_ton(context, o, tx_hash):
    charge = "ton_" + tx_hash
    status, order = await asyncio.to_thread(mark_paid, o["id"], charge)
    await finalize_paid(context, o["user_id"], o["id"], status, order, charge, stars=False)
    return status


async def watch_ton(context, oid):
    try:
        wallet = await asyncio.to_thread(get_wallet)
        for _ in range(60):
            await asyncio.sleep(10)
            o = await asyncio.to_thread(get_order, oid)
            if not o or o["status"] != "pending":
                return
            h = await find_ton_payment(wallet, oid, o.get("ton_nano"), o.get("created_at"))
            if h:
                await settle_ton(context, o, h)
                return
    except Exception as e:
        log.error("watch_ton failed: %s", e)


async def ton_check_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    try:
        oid = int(q.data.split("_")[1])
    except Exception:
        await q.answer()
        return
    o = get_order(oid)
    if not o or o["user_id"] != q.from_user.id:
        await q.answer(T("ton_invalid_order"), show_alert=True)
        return
    if o["status"] in ("paid", "shipped"):
        await q.answer(T("ton_already"), show_alert=True)
        return
    h = await find_ton_payment(get_wallet(), oid, o.get("ton_nano"), o.get("created_at"))
    if not h:
        await q.answer(T("ton_not_found") if o["status"] == "pending" else T("ton_invalid_order"),
                       show_alert=True)
        return
    await q.answer()
    await settle_ton(context, o, h)


async def user_says_paid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with cursor() as cur:
        cur.execute(
            "SELECT * FROM shop_orders WHERE shop_id=? AND user_id=? AND status='pending' "
            "AND ton_nano IS NOT NULL ORDER BY id DESC LIMIT 1",
            (shop_id(), update.effective_user.id),
        )
        o = cur.fetchone()
    if not o:
        await update.message.reply_text(T("ton_no_pending"))
        return
    h = await find_ton_payment(get_wallet(), o["id"], o.get("ton_nano"), o.get("created_at"))
    if not h:
        await update.message.reply_text(T("ton_not_found"))
        return
    await settle_ton(context, o, h)


# ── TON: محفظة المتجر (أدمن) ──
async def cmd_setwallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return ConversationHandler.END
    if context.args:
        w = context.args[0].strip()
        if not WALLET_RE.match(w):
            await update.message.reply_text(T("wallet_bad"))
            return ConversationHandler.END
        set_wallet(w)
        await update.message.reply_text(T("wallet_ok"))
        return ConversationHandler.END
    context.user_data["adm"] = {"mode": "wallet"}
    await update.message.reply_text(T("wallet_ask", w=esc(get_wallet() or "—")))
    return W_ADDR


async def wallet_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        return ConversationHandler.END
    context.user_data["adm"] = {"mode": "wallet"}
    await q.message.reply_text(T("wallet_ask", w=esc(get_wallet() or "—")))
    return W_ADDR


async def got_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    w = update.message.text.strip()
    if not WALLET_RE.match(w):
        await update.message.reply_text(T("wallet_bad"))
        return W_ADDR
    set_wallet(w)
    context.user_data.pop("adm", None)
    await update.message.reply_text(T("wallet_ok"))
    return ConversationHandler.END


# ═════════════════════════════════════════════════════════════════════════════
# 9) لوحة الأدمن: المنتجات
# ═════════════════════════════════════════════════════════════════════════════
def adm(context):
    return context.user_data.setdefault("adm", {})


def panel_text(p):
    n = count_media(p["id"])
    t = (
        f"🗂 <b>{esc(p['name'])}</b>  (#{p['id']})\n\n"
        f"{T('pt_type_ship') if p['shipping'] else T('pt_type_dig')}\n"
        f"{T('pt_price', p=p['price']) if stars_on(p) else T('pt_stars_off')}\n"
    )
    t += (T("pt_ton", p=f"{fmt_ton(p['price_ton'])} TON") if p.get("price_ton") else T("pt_ton_off")) + "\n"
    t += (
        f"{T('pt_stock', s=stock_label(p['stock']))}\n"
        f"{T('pt_media', n=n)}\n"
        f"{T('pt_visible') if p['active'] else T('pt_hidden')}\n"
    )
    extras = list_extras(p["id"])
    if extras:
        t += T("pt_extras", n=len(extras)) + "\n"
        for f in extras:
            t += f"    ↳ <b>{esc(f['label'])}</b> ({kind_label(f)})\n"
            for r in extra_rules(f):
                t += f"        • {r}\n"
    if not p["shipping"]:
        t += T("pt_deliv", a="✅" if p["delivery_text"] else "❌", b="✅" if p["delivery_file"] else "❌") + "\n"
    if p["description"]:
        t += f"\n📝 {esc(p['description'][:200])}"
    return t


def panel_markup(p):
    i = p["id"]
    B = InlineKeyboardButton
    rows = [
        [B(T("b_name"), callback_data=f"edt_name_{i}"),
         B(T("b_desc"), callback_data=f"edt_desc_{i}")],
        [B(T("b_price"), callback_data=f"edt_price_{i}"),
         B(T("b_ton"), callback_data=f"edt_ton_{i}")],
        [B(T("b_stock"), callback_data=f"edt_stock_{i}")],
        [B(T("b_photos"), callback_data=f"edt_photos_{i}"),
         B(T("b_clr"), callback_data=f"adm_clrphotos_{i}")],
        [B(T("b_lib"), callback_data=f"mpick_p{i}_0")],
        [B(T("b_to_dig") if p["shipping"] else T("b_to_phys"), callback_data=f"adm_ship_{i}")],
    ]
    if p.get("price_ton") and stars_on(p):
        rows.append([B(T("b_off_stars"), callback_data=f"adm_offstars_{i}"),
                     B(T("b_off_ton"), callback_data=f"adm_offton_{i}")])
    if not p["shipping"]:
        rows.append([B(T("b_deliv"), callback_data=f"edt_deliv_{i}")])
    rows.append([B(T("b_extra", n=len(list_extras(i))), callback_data=f"xl_{i}")])
    rows.append([B(T("b_hide") if p["active"] else T("b_show"), callback_data=f"adm_active_{i}"),
                 B(T("b_del"), callback_data=f"adm_del_{i}")])
    rows.append([B(T("b_back"), callback_data="adm_list")])
    return InlineKeyboardMarkup(rows)


async def render_panel(message, pid, edit=False):
    p = get_product(pid)
    if not p:
        await message.reply_text(T("not_found"))
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
        f"{'✅' if p['active'] else '🚫'} {'🛠' if p['shipping'] else '💾'} {p['name'][:30]} — {price_label(p)}",
        callback_data=f"adm_panel_{p['id']}")] for p in prods]
    rows.append([InlineKeyboardButton(T("b_add"), callback_data="adm_new")])
    rows.append([InlineKeyboardButton(T("b_lm"), callback_data="lm_open")])
    rows.append([InlineKeyboardButton(T("b_mlib"), callback_data="mpick_x_0")])
    rows.append([InlineKeyboardButton(T("b_wallet"), callback_data="adm_wallet")])
    await message.reply_text(T("products_title"), reply_markup=InlineKeyboardMarkup(rows))


async def cmd_products(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    await send_products_admin(update.message)


async def adm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        return
    parts = q.data.split("_")
    action, pid = parts[1], int(parts[2])
    p = get_product(pid)
    if not p:
        await q.message.reply_text(T("not_found"))
        return

    if action == "panel":
        await render_panel(q.message, pid)
    elif action == "ship":
        new = 0 if p["shipping"] else 1
        stock = p["stock"]
        if new == 0 and stock == 0:
            stock = -1
        if new == 1 and stock < 0:
            stock = 1
        update_product(pid, shipping=new, stock=stock)
        await render_panel(q.message, pid, edit=True)
    elif action == "active":
        update_product(pid, active=0 if p["active"] else 1)
        await render_panel(q.message, pid, edit=True)
    elif action == "offstars":
        if p.get("price_ton"):
            update_product(pid, stars_on=0)
            await render_panel(q.message, pid, edit=True)
        else:
            await q.message.reply_text(T("need_one"))
    elif action == "offton":
        if stars_on(p):
            update_product(pid, price_ton=None)
            await render_panel(q.message, pid, edit=True)
        else:
            await q.message.reply_text(T("need_one"))
    elif action == "clrphotos":
        clear_photos(pid)
        await render_panel(q.message, pid, edit=True)
    elif action == "del":
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(T("b_yes_del"), callback_data=f"adm_delyes_{pid}"),
            InlineKeyboardButton(T("b_no"), callback_data=f"adm_panel_{pid}"),
        ]])
        await q.message.reply_text(T("del_ask", name=esc(p["name"])), reply_markup=kb)
    elif action == "delyes":
        delete_product(pid)
        await q.message.edit_text(T("deleted"))


async def adm_list_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if is_admin(update):
        await send_products_admin(q.message)


# ── إضافة منتج (محادثة) ──
async def begin_new(message, context):
    context.user_data["adm"] = {"mode": "new"}
    await message.reply_text(T("new_title"))
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
        await update.message.reply_text(T("bad_name"))
        return A_NAME
    adm(context)["name"] = name
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(T("b_pm_stars"), callback_data="pm_stars")],
        [InlineKeyboardButton(T("b_pm_ton"), callback_data="pm_ton")],
        [InlineKeyboardButton(T("b_pm_both"), callback_data="pm_both")],
    ])
    await update.message.reply_text(T("pick_method"), reply_markup=kb)
    return A_METHOD


async def got_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(update):
        await q.answer()
        return ConversationHandler.END
    m = q.data.split("_")[1]
    if m in ("ton", "both") and not get_wallet():
        await q.answer(T("wallet_needed"), show_alert=True)
        return A_METHOD
    await q.answer()
    adm(context)["method"] = m
    if m == "ton":
        await q.message.reply_text(T("ask_ton"))
        return A_TON
    await q.message.reply_text(T("ask_price"))
    return A_PRICE


async def ask_type(message):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(T("b_t_ship"), callback_data="nt_ship")],
        [InlineKeyboardButton(T("b_t_dig"), callback_data="nt_dig")],
    ])
    await message.reply_text(T("pick_type"), reply_markup=kb)
    return A_TYPE


async def got_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if not t.isdigit() or not (1 <= int(t) <= 1000000):
        await update.message.reply_text(T("bad_price"))
        return A_PRICE
    adm(context)["price"] = int(t)
    if adm(context).get("method") == "both":
        await update.message.reply_text(T("ask_ton"))
        return A_TON
    return await ask_type(update.message)


async def got_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ship = 1 if q.data == "nt_ship" else 0
    adm(context)["shipping"] = ship
    await q.message.reply_text(T("ask_stock_ship" if ship else "ask_stock_dig"))
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
        await update.message.reply_text(T("bad_int"))
        return A_STOCK
    adm(context)["stock"] = n
    await update.message.reply_text(T("ask_desc"))
    return A_DESC


async def got_ton(update: Update, context: ContextTypes.DEFAULT_TYPE):
    v = parse_ton_price(update.message.text)
    if v is None:
        await update.message.reply_text(T("bad_ton"))
        return A_TON
    adm(context)["price_ton"] = v
    return await ask_type(update.message)


async def create_draft(update, context, desc):
    a = adm(context)
    a["pid"] = add_product(a["name"], desc, a.get("price", 0), a["stock"], a["shipping"], active=0)
    update_product(a["pid"], price_ton=a.get("price_ton"), stars_on=0 if a.get("method") == "ton" else 1)
    await update.message.reply_text(T("ask_media", n=MAX_PHOTOS))
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
    n = count_media(pid)
    if n >= MAX_PHOTOS:
        await update.message.reply_text(T("media_max", n=MAX_PHOTOS))
        return A_PHOTOS
    if update.message.video:
        add_photo(pid, update.message.video.file_id, "video")
        lib_add(update.message.video.file_id, update.message.video.file_unique_id, "video")
        what = T("what_video")
    else:
        add_photo(pid, update.message.photo[-1].file_id, "photo")
        lib_add(update.message.photo[-1].file_id, update.message.photo[-1].file_unique_id, "photo")
        what = T("what_photo")
    await update.message.reply_text(T("media_added", what=what, i=n + 1, n=MAX_PHOTOS))
    return A_PHOTOS


async def photos_wrong(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(T("media_wrong"))
    return A_PHOTOS


async def finish_admin(update, context):
    a = adm(context)
    pid = a.get("pid")
    if a.get("mode") == "new" and pid:
        update_product(pid, active=1)
        await update.effective_message.reply_text(T("saved_published"))
    context.user_data.pop("adm", None)
    if pid:
        await render_panel(update.effective_message, pid)
    return ConversationHandler.END


async def photos_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    p = get_product(a["pid"])
    if a.get("mode") == "new" and p and not p["shipping"]:
        await update.message.reply_text(T("ask_delivery"))
        return A_DELIV
    return await finish_admin(update, context)


async def got_deliv_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_product(adm(context)["pid"], delivery_text=update.message.text.strip()[:3000])
    await update.message.reply_text(T("deliv_text_saved"))
    return A_DELIV


async def got_deliv_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_product(adm(context)["pid"], delivery_file=update.message.document.file_id)
    await update.message.reply_text(T("deliv_file_saved"))
    return A_DELIV


async def deliv_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await finish_admin(update, context)


# ── تعديل منتج ──
async def edit_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        return ConversationHandler.END
    _, field, pid = q.data.split("_")
    pid = int(pid)
    if not get_product(pid):
        await q.message.reply_text(T("not_found"))
        return ConversationHandler.END
    context.user_data["adm"] = {"mode": "edit", "pid": pid, "field": field}

    if field == "photos":
        context.user_data["adm"]["replace"] = True
        await q.message.reply_text(T("photos_replace", n=MAX_PHOTOS))
        return A_PHOTOS
    if field == "deliv":
        await q.message.reply_text(T("ask_delivery"))
        return A_DELIV
    await q.message.reply_text(T("ep_" + field) + "\n" + T("cancel_hint"))
    return E_VALUE


async def got_edit_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    pid, field = a["pid"], a["field"]
    p = get_product(pid)
    if not p:
        await update.message.reply_text(T("not_found"))
        return ConversationHandler.END
    t = update.message.text.strip()

    if field == "name":
        if not (2 <= len(t) <= 100):
            await update.message.reply_text(T("bad_name"))
            return E_VALUE
        update_product(pid, name=t)
    elif field == "desc":
        update_product(pid, description="" if t == "-" else t[:700])
    elif field == "price":
        if not t.isdigit() or not (1 <= int(t) <= 1000000):
            await update.message.reply_text(T("bad_price"))
            return E_VALUE
        update_product(pid, price=int(t), stars_on=1)
    elif field == "ton":
        if t == "-":
            if not stars_on(p):
                await update.message.reply_text(T("need_one"))
                return E_VALUE
            update_product(pid, price_ton=None)
        else:
            if not get_wallet():
                context.user_data.pop("adm", None)
                await update.message.reply_text(T("wallet_needed"))
                return ConversationHandler.END
            v = parse_ton_price(t)
            if v is None:
                await update.message.reply_text(T("bad_ton"))
                return E_VALUE
            update_product(pid, price_ton=v)
    elif field == "stock":
        n = parse_stock(t, p["shipping"])
        if n is None:
            await update.message.reply_text(T("bad_int"))
            return E_VALUE
        update_product(pid, stock=n)

    context.user_data.pop("adm", None)
    await update.message.reply_text(T("updated"))
    await render_panel(update.message, pid)
    return ConversationHandler.END


# ── حقول الزبون (لوحة الأدمن) ──
def extras_markup(pid, extras):
    B = InlineKeyboardButton
    rows = []
    for f in extras:
        icon = "🔢" if f.get("kind") == "number" else "🔤"
        rows.append([B(f"{icon} {f['label'][:40]}", callback_data=f"xf_{pid}_{f['id']}")])
    if len(extras) < MAX_EXTRAS:
        rows.append([B(T("b_x_add"), callback_data=f"xpick_{pid}")])
    rows.append([B(T("b_x_prod"), callback_data=f"adm_panel_{pid}")])
    return InlineKeyboardMarkup(rows)


async def _show(message, text, markup, edit=True):
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup)
            return
        except Exception:
            pass
    await message.reply_text(text, reply_markup=markup)


async def render_extras(message, pid, edit=False):
    p = get_product(pid)
    if not p:
        await message.reply_text(T("not_found"))
        return
    extras = list_extras(pid)
    text = T("x_list_title", name=esc(p["name"]))
    if not extras:
        text += "\n\n" + T("x_none")
    await _show(message, text, extras_markup(pid, extras), edit)


def field_text(f):
    t = f"🧩 <b>{esc(f['label'])}</b>\n{T('x_field_kind', kind=kind_label(f))}\n"
    rules = extra_rules(f)
    t += ("\n".join(rules) if rules else T("x_field_norules")) + "\n\n"
    t += T("x_field_used", n=field_usage(f["id"]))
    return t


async def x_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        return
    parts = q.data.split("_")
    action, nums = parts[0], [int(x) for x in parts[1:]]
    pid = nums[0]

    if action == "xl":
        await render_extras(q.message, pid, edit=True)
        return
    p = get_product(pid)
    if not p:
        await q.message.reply_text(T("not_found"))
        return
    B = InlineKeyboardButton

    if action == "xpick":
        used = {f["id"] for f in list_extras(pid)}
        lib = [f for f in list_library() if f["id"] not in used]
        rows = [[B(f"{'🔢' if f.get('kind') == 'number' else '🔤'} {f['label'][:40]}",
                   callback_data=f"xat_{pid}_{f['id']}")] for f in lib]
        rows.append([B(T("b_x_new"), callback_data=f"xnew_{pid}")])
        rows.append([B(T("b_x_back"), callback_data=f"xl_{pid}")])
        text = T("x_pick_title", name=esc(p["name"]))
        if not lib:
            text += "\n\n" + T("x_pick_none")
        await _show(q.message, text, InlineKeyboardMarkup(rows))
        return

    fid = nums[1]
    if action == "xat":
        if len(list_extras(pid)) >= MAX_EXTRAS:
            await q.message.reply_text(T("x_max", n=MAX_EXTRAS))
            return
        attach_extra(pid, fid)
        await render_extras(q.message, pid, edit=True)
        return

    f = get_extra(fid)
    if not f:
        await q.message.reply_text(T("not_found"))
        return
    if action == "xf":
        kb = InlineKeyboardMarkup([
            [B(T("b_x_edit"), callback_data=f"xedit_{pid}_{fid}")],
            [B(T("b_x_unlink"), callback_data=f"xun_{pid}_{fid}")],
            [B(T("b_x_kill"), callback_data=f"xkill_{pid}_{fid}")],
            [B(T("b_x_back"), callback_data=f"xl_{pid}")],
        ])
        await _show(q.message, field_text(f), kb)
    elif action == "xun":
        detach_extra(pid, fid)
        await render_extras(q.message, pid, edit=True)
    elif action == "xkill":
        delete_extra(fid)
        await render_extras(q.message, pid, edit=True)


async def x_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(update):
        await q.answer()
        return ConversationHandler.END
    parts = q.data.split("_")
    action, pid = parts[0], int(parts[1])
    if not get_product(pid):
        await q.answer()
        await q.message.reply_text(T("not_found"))
        return ConversationHandler.END

    if action == "xnew":
        if len(list_extras(pid)) >= MAX_EXTRAS:
            await q.answer(T("x_max", n=MAX_EXTRAS), show_alert=True)
            return ConversationHandler.END
        context.user_data["adm"] = {"mode": "xfield", "pid": pid, "xid": None}
    else:
        fid = int(parts[2])
        if not get_extra(fid):
            await q.answer()
            await q.message.reply_text(T("not_found"))
            return ConversationHandler.END
        context.user_data["adm"] = {"mode": "xfield", "pid": pid, "xid": fid}

    await q.answer()
    await q.message.reply_text(T("x_ask"))
    return X_LABEL


async def got_x_label(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    t = update.message.text.strip()
    if not (2 <= len(t) <= 200):
        await update.message.reply_text(T("x_bad_len"))
        return X_LABEL
    a["xlabel"] = t
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(T("b_x_num"), callback_data="xt_num"),
        InlineKeyboardButton(T("b_x_text"), callback_data="xt_text"),
    ]])
    await update.message.reply_text(T("x_type_ask"), reply_markup=kb)
    return X_TYPE


async def got_x_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        return ConversationHandler.END
    a = adm(context)
    pid = a.get("pid")
    if not pid or not a.get("xlabel"):
        return ConversationHandler.END
    a["xtype"] = "number" if q.data == "xt_num" else "text"
    await q.message.reply_text(T("x_len_ask"))
    return X_LEN


def _x_norm(a, t):
    if a.get("xtype") == "number":
        return t.translate(AR_DIGITS).replace(" ", "")
    return t


def _x_max_len(a):
    return MAX_EXTRA_LEN if a.get("xtype") == "number" else 300


async def got_x_len(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    t = update.message.text.strip().translate(AR_DIGITS)
    if t == "-":
        a["xlen"] = 0
    else:
        mx = _x_max_len(a)
        if not t.isdigit() or not (1 <= int(t) <= mx):
            await update.message.reply_text(T("x_len_bad", max=mx))
            return X_LEN
        a["xlen"] = int(t)
    await update.message.reply_text(T("x_pre_ask"))
    return X_PRE


async def got_x_pre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    t = update.message.text.strip()
    if t == "-":
        a["xpre"] = None
    else:
        t = _x_norm(a, t)
        if a.get("xtype") == "number" and not t.isdigit():
            await update.message.reply_text(T("x_digits_only"))
            return X_PRE
        if not t or len(t) > 100:
            await update.message.reply_text(T("x_bad_len"))
            return X_PRE
        if a.get("xlen") and len(t) > a["xlen"]:
            await update.message.reply_text(T("x_rules_long", n=a["xlen"]))
            return X_PRE
        a["xpre"] = t
    await update.message.reply_text(T("x_suf_ask"))
    return X_SUF


async def got_x_suf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    pid = a.get("pid")
    if not pid or not a.get("xlabel"):
        return ConversationHandler.END
    t = update.message.text.strip()
    if t == "-":
        suf = None
    else:
        t = _x_norm(a, t)
        if a.get("xtype") == "number" and not t.isdigit():
            await update.message.reply_text(T("x_digits_only"))
            return X_SUF
        if not t or len(t) > 100:
            await update.message.reply_text(T("x_bad_len"))
            return X_SUF
        if a.get("xlen") and len(a.get("xpre") or "") + len(t) > a["xlen"]:
            await update.message.reply_text(T("x_rules_long", n=a["xlen"]))
            return X_SUF
        suf = t

    kind = a.get("xtype", "text")
    flen = a.get("xlen") or 0
    xid = a.get("xid")
    if xid and get_extra(xid):
        update_extra(xid, a["xlabel"], kind, flen, a.get("xpre"), suf)
    else:
        if len(list_extras(pid)) >= MAX_EXTRAS:
            context.user_data.pop("adm", None)
            await update.message.reply_text(T("x_max", n=MAX_EXTRAS))
            return ConversationHandler.END
        xid = add_extra(a["xlabel"], kind, flen, a.get("xpre"), suf)
        attach_extra(pid, xid)

    context.user_data.pop("adm", None)
    await update.message.reply_text(T("x_saved"))
    await render_extras(update.message, pid)
    return ConversationHandler.END


# ── مكتبة الوسائط ووسائط قائمة المنتجات (أدمن) ──
async def show_lm_panel(message, edit=False):
    B = InlineKeyboardButton
    kb = InlineKeyboardMarkup([
        [B(T("b_lm_up"), callback_data="mup_l"), B(T("b_lm_lib"), callback_data="mpick_l_0")],
        [B(T("b_lm_clear"), callback_data="lm_clear")],
        [B(T("b_back"), callback_data="adm_list")],
    ])
    await _show(message, T("lm_title", n=len(get_list_media())), kb, edit)


async def send_library_page(message, target, page):
    manage = target == "x"
    total = lib_count()
    items = lib_list(LIB_PAGE, page * LIB_PAGE)
    if not items and page > 0:
        return await send_library_page(message, target, 0)
    B = InlineKeyboardButton

    if items:
        try:
            if len(items) == 1:
                m = items[0]
                send = message.reply_video if m["kind"] == "video" else message.reply_photo
                await send(m["file_id"], caption=f"#{m['id']}")
            else:
                await message.reply_media_group([
                    (InputMediaVideo if m["kind"] == "video" else InputMediaPhoto)(m["file_id"], caption=f"#{m['id']}")
                    for m in items
                ])
        except Exception as e:
            log.warning("library preview failed: %s", e)

    rows, row = [], []
    for m in items:
        icon = "🗑" if manage else ("🎬" if m["kind"] == "video" else "🖼")
        cb = f"mdel_{m['id']}_{page}" if manage else f"madd_{target}_{m['id']}"
        row.append(B(f"{icon} #{m['id']}", callback_data=cb))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    nav = []
    if page > 0:
        nav.append(B("◀️", callback_data=f"mpick_{target}_{page - 1}"))
    if (page + 1) * LIB_PAGE < total:
        nav.append(B("▶️", callback_data=f"mpick_{target}_{page + 1}"))
    if nav:
        rows.append(nav)
    if manage:
        rows.append([B(T("b_lib_up"), callback_data="mup_x")])
        rows.append([B(T("b_back"), callback_data="adm_list")])
    else:
        back = "lm_open" if target == "l" else f"adm_panel_{target[1:]}"
        rows.append([B(T("b_done"), callback_data=back)])

    text = T("lib_title_manage" if manage else "lib_title_pick", total=total)
    if not items:
        text += "\n\n" + T("lib_empty")
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(rows))


async def media_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin(update):
        await q.answer()
        return
    toast = None
    data = q.data
    parts = data.split("_")

    if data == "lm_open":
        await show_lm_panel(q.message)
    elif data == "lm_clear":
        set_list_media([])
        await show_lm_panel(q.message, edit=True)
    elif parts[0] == "mpick":
        await send_library_page(q.message, parts[1], int(parts[2]))
    elif parts[0] == "mdel":
        lib_delete(int(parts[1]))
        toast = T("lib_deleted")
        await send_library_page(q.message, "x", int(parts[2]))
    elif parts[0] == "madd":
        target, mid = parts[1], int(parts[2])
        m = lib_get(mid)
        if not m:
            toast = T("not_found")
        elif target == "l":
            items = get_list_media()
            if any(x["file_id"] == m["file_id"] for x in items):
                toast = T("lib_dup")
            elif len(items) >= MAX_PHOTOS:
                toast = T("media_max", n=MAX_PHOTOS)
            else:
                items.append({"file_id": m["file_id"], "kind": m["kind"] or "photo"})
                set_list_media(items)
                toast = T("lib_added", i=len(items), n=MAX_PHOTOS)
        else:
            pid = int(target[1:])
            if not get_product(pid):
                toast = T("not_found")
            else:
                cur_media = get_media(pid)
                if any(x["file_id"] == m["file_id"] for x in cur_media):
                    toast = T("lib_dup")
                elif len(cur_media) >= MAX_PHOTOS:
                    toast = T("media_max", n=MAX_PHOTOS)
                else:
                    add_photo(pid, m["file_id"], m["kind"] or "photo")
                    toast = T("lib_added", i=len(cur_media) + 1, n=MAX_PHOTOS)
    await q.answer(toast)


async def mup_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(update):
        return ConversationHandler.END
    kind = q.data.split("_")[1]
    context.user_data["adm"] = {"mode": "listmedia" if kind == "l" else "library"}
    await q.message.reply_text(T("mup_ask_l" if kind == "l" else "mup_ask_x"))
    return M_UP


async def got_lmedia(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    msg = update.message
    if msg.video:
        fid, uid, kind = msg.video.file_id, msg.video.file_unique_id, "video"
    else:
        ph = msg.photo[-1]
        fid, uid, kind = ph.file_id, ph.file_unique_id, "photo"
    lib_add(fid, uid, kind)
    if a.get("mode") == "listmedia":
        items = get_list_media()
        if len(items) >= MAX_PHOTOS:
            await msg.reply_text(T("media_max", n=MAX_PHOTOS))
            return M_UP
        if not any(x["file_id"] == fid for x in items):
            items.append({"file_id": fid, "kind": kind})
            set_list_media(items)
        await msg.reply_text(T("lm_saved", i=len(items), n=MAX_PHOTOS))
    else:
        await msg.reply_text(T("lib_saved"))
    return M_UP


async def lmedia_wrong(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(T("media_wrong"))
    return M_UP


async def lmedia_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = context.user_data.pop("adm", {})
    if a.get("mode") == "listmedia":
        await show_lm_panel(update.message)
    else:
        await send_products_admin(update.message)
    return ConversationHandler.END


async def adm_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = context.user_data.pop("adm", {})
    if a.get("mode") == "new" and a.get("pid"):
        delete_product(a["pid"])
    await update.message.reply_text(T("adm_cancelled"))
    return ConversationHandler.END


# ═════════════════════════════════════════════════════════════════════════════
# 10) أوامر الأدمن: الطلبات والمساعدة
# ═════════════════════════════════════════════════════════════════════════════
async def cmd_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    orders = list_orders()
    if not orders:
        await update.message.reply_text(T("no_orders"))
        return
    for o in orders:
        await update.message.reply_text(order_text(o))


async def cmd_shipped(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(T("shipped_usage"))
        return
    o = get_order(int(context.args[0]))
    if not o or o["status"] != "paid":
        await update.message.reply_text(T("shipped_bad"))
        return
    set_order(o["id"], "shipped")
    await update.message.reply_text(T("shipped_ok", id=o["id"]))
    if o.get("wilaya"):
        try:
            await context.bot.send_message(o["user_id"], T("shipped_user", id=o["id"]))
        except Exception:
            pass


async def cmd_refund(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(T("refund_usage"))
        return
    o = get_order(int(context.args[0]))
    if not o or o["status"] not in ("paid", "shipped") or not o["charge_id"]:
        await update.message.reply_text(T("refund_bad"))
        return
    if str(o["charge_id"]).startswith("ton_"):
        await update.message.reply_text(T("refund_bad"))
        return
    try:
        await context.bot.refund_star_payment(user_id=o["user_id"], telegram_payment_charge_id=o["charge_id"])
    except Exception as e:
        await update.message.reply_text(T("refund_fail", err=esc(e)))
        return
    if o["status"] == "paid" and o.get("product_id"):
        restock_one(o["product_id"])
    set_order(o["id"], "refunded")
    await update.message.reply_text(T("refund_ok", amount=o["amount"], id=o["id"]))
    try:
        await context.bot.send_message(o["user_id"], T("refunded_user", id=o["id"]))
    except Exception:
        pass


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    t = T("help_admin") + T("help_ton")
    if shop_id() == 0:
        t += T("help_multi")
    await update.message.reply_text(t)


# ═════════════════════════════════════════════════════════════════════════════
# 11) إدارة البوتات المتعددة (البوت الرئيسي فقط)
# ═════════════════════════════════════════════════════════════════════════════
async def got_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if shop_id() != 0 or not is_admin(update):
        return
    token = update.message.text.strip()
    try:
        await update.message.delete()
    except Exception:
        pass
    chat = update.effective_chat.id

    if token == BOT_TOKEN:
        await context.bot.send_message(chat, T("tok_main"))
        return
    try:
        async with Bot(token) as b:
            me = await b.get_me()
    except Exception as e:
        await context.bot.send_message(chat, T("tok_invalid", err=esc(str(e)[:150])))
        return

    owner = update.effective_user.id
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
        await context.bot.send_message(chat, T("tok_fail", err=esc(str(e)[:200])))
        return

    await context.bot.send_message(chat, T("tok_ok", u=esc(me.username), id=me.id))


async def cmd_bots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if shop_id() != 0 or not is_admin(update):
        return
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_bots ORDER BY created_at")
        rows = cur.fetchall()
    if not rows:
        await update.message.reply_text(T("bots_none"))
        return
    t = T("bots_title")
    for r in rows:
        live = T("st_run") if r["bot_id"] in RUNNING else (T("st_stop") if r["active"] else T("st_off"))
        t += f"{live} — @{esc(r['username'])} — ID: <code>{r['bot_id']}</code>\n"
    await update.message.reply_text(t)


async def cmd_delbot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if shop_id() != 0 or not is_admin(update):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(T("delbot_usage"))
        return
    bid = int(context.args[0])
    with cursor() as cur:
        cur.execute("SELECT * FROM shop_bots WHERE bot_id=?", (bid,))
        r = cur.fetchone()
    if not r:
        await update.message.reply_text(T("delbot_nf"))
        return
    await stop_shop(bid)
    with cursor() as cur:
        cur.execute("UPDATE shop_bots SET active=0 WHERE bot_id=?", (bid,))
    await update.message.reply_text(T("delbot_ok", u=esc(r["username"])))


async def start_shop(token, shop):
    app = build_app(token, shop, is_main=False)
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    RUNNING[shop["id"]] = app
    await setup_commands(app.bot, shop["admin"], shop["id"])
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


# ═════════════════════════════════════════════════════════════════════════════
# 12) الموقع: Telegram Mini App — منتجات فقط، بدون صور ولا فيديو
# ═════════════════════════════════════════════════════════════════════════════
MAIN_APP = None
WEB_RUNNER = None
TG_JS = {"data": None, "ts": 0.0, "fail": 0.0}
TG_JS_URL = "https://telegram.org/js/telegram-web-app.js"


def webapp_url(sid):
    return f"{WEBAPP_URL}/app/{sid}"


def webapp_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T("btn_open_shop"), web_app=WebAppInfo(url=webapp_url(shop_id())))],
        [InlineKeyboardButton(T("btn_classic"), callback_data="list")],
    ])


def shop_app(sid):
    return MAIN_APP if sid == 0 else RUNNING.get(sid)


def plain(s):
    return html.unescape(re.sub(r"<[^>]+>", "", str(s)))


def verify_init_data(init_data, token, max_age=86400):
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        got = pairs.pop("hash", "")
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
        calc = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        if not got or not hmac.compare_digest(calc, got):
            return None
        if time.time() - int(pairs.get("auth_date", "0")) > max_age:
            return None
        u = json.loads(pairs.get("user", "{}"))
        if not u.get("id"):
            return None
        name = " ".join(x for x in (u.get("first_name"), u.get("last_name")) if x)
        return SimpleNamespace(id=int(u["id"]), username=u.get("username") or "", full_name=name)
    except Exception:
        return None


class _Ctx:
    def __init__(self, bot):
        self.bot = bot


def ton_comment_payload(text):
    data = b"\x00\x00\x00\x00" + text.encode()
    cell = bytes([0, len(data) * 2]) + data
    boc = bytes([0xB5, 0xEE, 0x9C, 0x72, 0x01, 0x01, 0x01, 0x01, 0x00, len(cell), 0x00]) + cell
    return base64.b64encode(boc).decode()


def _safe_dumps(o):
    return json.dumps(o, default=str, ensure_ascii=False)


def jresp(data, status=200):
    return web.json_response(data, status=status, dumps=_safe_dumps)


def load_home_data(uid, sid):
    """يبني قائمة المنتجات (نصوص وأسعار فقط). كل جزء محمي بحيث لا يسقط الموقع أبداً."""
    prods = list_products()
    try:
        has_wallet = bool(get_wallet())
    except Exception as e:
        log.warning("wallet check failed: %s", e)
        has_wallet = False

    extras, held = {}, {}
    try:
        with cursor() as cur:
            cur.execute("SELECT l.product_id, f.id, f.label, f.kind, f.flen, f.prefix, f.suffix "
                        "FROM shop_product_fields l JOIN shop_fields f ON f.id=l.field_id "
                        "WHERE f.shop_id=? ORDER BY l.id", (sid,))
            for r in cur.fetchall():
                extras.setdefault(r["product_id"], []).append(r)
    except Exception as e:
        log.warning("home extras query failed: %s", e)
    try:
        with cursor() as cur:
            cur.execute("SELECT product_id, COUNT(*) AS n FROM shop_orders WHERE shop_id=? AND status='pending' "
                        "AND reserved_until > ? AND user_id <> ? GROUP BY product_id", (sid, int(time.time()), uid))
            for r in cur.fetchall():
                held[r["product_id"]] = r["n"]
    except Exception as e:
        log.warning("home held query failed: %s", e)

    out = []
    for p in prods:
        try:
            methods = []
            if stars_on(p) and (p.get("price") or 0) > 0:
                methods.append("stars")
            if p.get("price_ton") and has_wallet:
                methods.append("ton")
            stock = p.get("stock")
            stock = 0 if stock is None else stock
            if stock == 0:
                state = "sold"
            elif stock < 0 or stock - held.get(p["id"], 0) > 0:
                state = "available"
            else:
                state = "held"
            out.append({
                "id": p["id"], "name": p.get("name") or "", "description": p.get("description") or "",
                "shipping": bool(p.get("shipping")), "stock": stock, "methods": methods,
                "price": p.get("price") if "stars" in methods else None,
                "price_ton": fmt_ton(p["price_ton"]) if "ton" in methods else None,
                "state": state,
                "extras": [{"id": f["id"], "label": f["label"], "kind": f.get("kind") or "text",
                            "rules": [plain(r) for r in extra_rules(f)]} for f in extras.get(p["id"], [])],
            })
        except Exception as e:
            log.warning("skip product %s in home: %s", p.get("id"), e)
    return out


async def web_auth(request):
    try:
        sid = int(request.match_info["sid"])
    except Exception:
        raise web.HTTPNotFound()
    app = shop_app(sid)
    if not app:
        raise web.HTTPNotFound()
    user = verify_init_data(request.headers.get("X-Init-Data", ""), app.bot.token)
    if not user:
        raise web.HTTPUnauthorized()
    shop = app.bot_data["shop"]
    _shop.set(shop)
    lang = _SHOP_LANG.get(sid)
    if lang is None:
        lang = await asyncio.to_thread(get_shop_lang, sid)
    _lang.set(lang)
    return app, shop, user


def _err(code, msg, **extra):
    return jresp({"ok": False, "code": code, "error": plain(msg), **extra})


@web.middleware
async def err_mw(request, handler):
    """أي خطأ غير متوقع يُسجَّل ويرجع JSON منظم بدل صفحة 500 بيضاء."""
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("WEB ERROR on %s", request.path)
        return jresp({"ok": False, "code": "server", "error": "server"}, status=200)


async def web_health(request):
    return web.Response(text="ok")


_PAGE_GZ = None


async def web_page(request):
    global _PAGE_GZ
    try:
        sid = int(request.match_info["sid"])
    except Exception:
        raise web.HTTPNotFound()
    if not shop_app(sid):
        raise web.HTTPNotFound()
    if "gzip" in request.headers.get("Accept-Encoding", ""):
        if _PAGE_GZ is None:
            import gzip
            _PAGE_GZ = gzip.compress(WEB_HTML.encode("utf-8"), 9)
        return web.Response(body=_PAGE_GZ, headers={
            "Content-Type": "text/html; charset=utf-8", "Content-Encoding": "gzip",
            "Vary": "Accept-Encoding", "Cache-Control": "no-cache"})
    return web.Response(text=WEB_HTML, content_type="text/html", headers={"Cache-Control": "no-cache"})


async def fetch_tg_js():
    now = time.time()
    if TG_JS["data"] and now - TG_JS["ts"] < 6 * 3600:
        return TG_JS["data"]
    if now - TG_JS["fail"] < 120:
        return TG_JS["data"]
    try:
        async with httpx.AsyncClient(timeout=4, follow_redirects=True) as c:
            r = await c.get(TG_JS_URL)
        if r.status_code == 200 and r.content:
            TG_JS["data"], TG_JS["ts"] = r.content, now
        else:
            TG_JS["fail"] = now
    except Exception as e:
        TG_JS["fail"] = now
        log.warning("tg.js fetch failed: %s", e)
    return TG_JS["data"]


async def web_tgjs(request):
    data = await fetch_tg_js()
    if not data:
        raise web.HTTPFound(TG_JS_URL)
    return web.Response(body=data, content_type="application/javascript",
                        headers={"Cache-Control": "public, max-age=3600"})


async def web_home(request):
    app, shop, user = await web_auth(request)
    sid = shop["id"]

    def build():
        try:
            title = app.bot.first_name or "Shop"
        except Exception:
            title = "Shop"
        try:
            uname = app.bot.username or ""
        except Exception:
            uname = shop.get("username") or ""
        try:
            phone = get_phone(user.id)
        except Exception as e:
            log.warning("get_phone failed: %s", e)
            phone = None
        return {
            "ok": True,
            "lang": _lang.get(),
            "title": title,
            "bot": uname,
            "products": load_home_data(user.id, sid),
            "phone": phone,
            "wilayas": wilaya_names(),
        }

    data = await asyncio.to_thread(build)
    return jresp(data)


async def web_phone(request):
    app, shop, user = await web_auth(request)
    try:
        phone = await asyncio.to_thread(get_phone, user.id)
    except Exception as e:
        log.warning("phone failed: %s", e)
        phone = None
    return jresp({"phone": phone})


def _create_order_sync(user, data):
    try:
        pid = int(data.get("pid") or 0)
    except Exception:
        return ("bad_request", T("session_expired"), {}), None
    method = data.get("method")
    p = get_product(pid)
    if not p or not p["active"]:
        return ("gone", T("product_gone"), {}), None
    if method not in pay_methods(p):
        return ("method", T("no_pay_method"), {}), None
    state = availability(pid, user.id)
    if state != "available":
        return (state, T("unavail_" + state), {}), None

    phone = wilaya = address = None
    if p["shipping"]:
        phone = get_phone(user.id)
        if not phone:
            return ("phone", T("phone_ask"), {}), None
        if not (phone.startswith("213") and len(phone) == 12):
            return ("phone_dz", T("phone_not_dz"), {}), None
        try:
            idx = int(data.get("wilaya") or 0)
        except (TypeError, ValueError):
            idx = 0
        if not 1 <= idx <= len(WILAYAS["ar"]):
            return ("wilaya", T("pick_wilaya"), {}), None
        wilaya = f"{idx:02d} - {WILAYAS['ar'][idx - 1]} / {WILAYAS['en'][idx - 1]}"
        address = str(data.get("address") or "").strip()
        if not (10 <= len(address) <= 300):
            return ("address", T("addr_bad"), {}), None

    extras = list_extras(pid)
    vals = data.get("extras") or []
    if not isinstance(vals, list) or len(vals) != len(extras):
        return ("extras", T("session_expired"), {}), None
    pairs = []
    for i, (f, v) in enumerate(zip(extras, vals)):
        val, e = check_extra(f, str(v))
        if e:
            return ("extra", e, {"index": i}), None
        pairs.append({"label": f["label"], "value": val})

    oid, why = place_order(user, pid, phone, wilaya, address, pairs)
    if not oid:
        return (why, T("unavail_" + (why if why in ("sold", "held") else "sold")), {}), None
    order = get_order(oid)

    if method == "ton":
        wallet = get_wallet()
        if not (wallet and p.get("price_ton")):
            set_order(oid, "cancelled")
            return ("method", T("no_pay_method"), {}), None
        nano = to_nano(p["price_ton"])
        with cursor() as cur:
            cur.execute("UPDATE shop_orders SET ton_nano=? WHERE id=?", (nano, oid))
        return None, (p, oid, order, nano, wallet)
    return None, (p, oid, order, None, None)


async def web_order(request):
    app, shop, user = await web_auth(request)
    try:
        data = await request.json()
        if not isinstance(data, dict):
            raise ValueError("bad body")
    except Exception:
        return _err("bad_request", T("session_expired"))
    err, res = await asyncio.to_thread(_create_order_sync, user, data)
    if err:
        return _err(err[0], err[1], **err[2])
    p, oid, order, nano, wallet = res

    if data.get("method") == "stars":
        desc = T("invoice_ship_desc", name=p["name"]) if p["shipping"] else (p["description"] or p["name"])
        try:
            link = await app.bot.create_invoice_link(
                title=p["name"][:32], description=desc[:255], payload=f"order_{oid}",
                provider_token="", currency="XTR",
                prices=[LabeledPrice(p["name"][:32], order["amount"])],
            )
        except Exception as e:
            log.error("create_invoice_link failed: %s", e)
            await asyncio.to_thread(set_order, oid, "cancelled")
            return _err("invoice", T("session_expired"))
        return jresp({"ok": True, "oid": oid, "method": "stars", "invoice": link})

    memo = ton_memo(oid)
    task = asyncio.create_task(watch_ton(_Ctx(app.bot), oid))
    TON_TASKS.add(task)
    task.add_done_callback(TON_TASKS.discard)
    return jresp({"ok": True, "oid": oid, "method": "ton", "ton": {
        "address": wallet, "nano": str(nano), "amount": fmt_ton(nano / 1e9), "memo": memo,
        "payload": ton_comment_payload(memo),
        "url": f"https://app.tonkeeper.com/transfer/{wallet}?amount={nano}&text={memo}",
    }})


async def web_status(request):
    app, shop, user = await web_auth(request)
    o = await asyncio.to_thread(get_order, int(request.match_info["oid"]))
    if not o or o["user_id"] != user.id:
        raise web.HTTPNotFound()
    return jresp({"status": o["status"], "oid": o["id"]})


async def web_check(request):
    app, shop, user = await web_auth(request)
    oid = int(request.match_info["oid"])
    o = await asyncio.to_thread(get_order, oid)
    if not o or o["user_id"] != user.id:
        raise web.HTTPNotFound()
    if o["status"] == "pending" and o.get("ton_nano"):
        wallet = await asyncio.to_thread(get_wallet)
        h = await find_ton_payment(wallet, oid, o.get("ton_nano"), o.get("created_at"))
        if h:
            await settle_ton(_Ctx(app.bot), o, h)
            o = await asyncio.to_thread(get_order, oid)
    return jresp({"status": o["status"], "oid": oid})


async def web_manifest(request):
    return web.json_response(
        {"url": WEBAPP_URL, "name": "Shop", "iconUrl": f"{WEBAPP_URL}/logo.png"},
        headers={"Access-Control-Allow-Origin": "*"},
    )


async def web_logo(request):
    if not os.path.exists(LOGO_PATH):
        raise web.HTTPNotFound()
    return web.FileResponse(LOGO_PATH, headers={"Access-Control-Allow-Origin": "*"})


def web_app_factory():
    w = web.Application(middlewares=[err_mw])
    w.add_routes([
        web.get("/", web_health),
        web.get("/tg.js", web_tgjs),
        web.get(r"/app/{sid:\d+}", web_page),
        web.get(r"/api/{sid:\d+}/home", web_home),
        web.get(r"/api/{sid:\d+}/phone", web_phone),
        web.post(r"/api/{sid:\d+}/order", web_order),
        web.get(r"/api/{sid:\d+}/order/{oid:\d+}", web_status),
        web.post(r"/api/{sid:\d+}/order/{oid:\d+}/check", web_check),
        web.get("/tonconnect-manifest.json", web_manifest),
        web.get("/logo.png", web_logo),
    ])
    return w


async def start_web():
    global WEB_RUNNER, WEBAPP_URL
    if not WEBAPP_URL:
        log.info("WEBAPP_URL not set: Mini App disabled (classic bot flow only)")
        return
    if web is None:
        log.error("aiohttp is not installed -> Mini App disabled. Run: pip install aiohttp")
        WEBAPP_URL = ""
        return
    try:
        runner = web.AppRunner(web_app_factory())
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", PORT).start()
        WEB_RUNNER = runner
        log.info("Mini App listening on 0.0.0.0:%s (public URL: %s)", PORT, WEBAPP_URL)
        if not os.path.exists(LOGO_PATH):
            log.warning("Logo not found at %s (TON Connect icon). Put shop_logo.png next to bot.py", LOGO_PATH)
        asyncio.create_task(fetch_tg_js())
    except Exception as e:
        log.error("Mini App failed to start (%s) -> falling back to the classic bot flow", e)
        WEBAPP_URL = ""


async def stop_web():
    global WEB_RUNNER
    if WEB_RUNNER:
        try:
            await WEB_RUNNER.cleanup()
        except Exception as e:
            log.warning("stop web failed: %s", e)
        WEB_RUNNER = None


async def self_check():
    if not WEBAPP_URL:
        return "🚫 WEBAPP_URL / RAILWAY_PUBLIC_DOMAIN غير مضبوط"
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(WEBAPP_URL + "/")
        if r.status_code == 200 and r.text.strip() == "ok":
            return "✅ الموقع يستجيب"
        return f"⚠️ الرابط يرد HTTP {r.status_code} (تأكد أن Port الدومين في Railway = {PORT})"
    except Exception as e:
        return f"❌ تعذّر الوصول للرابط العام: {str(e)[:120]}"


async def cmd_diag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    chk = await self_check()
    tg_ok = "✅" if await fetch_tg_js() else "⚠️ (سيُستعمل telegram.org مباشرة)"
    try:
        n = len(await asyncio.to_thread(list_products))
        db = f"✅ ({n} منتج ظاهر)"
    except Exception as e:
        db = f"❌ {str(e)[:100]}"
    t = (
        "🩺 <b>Mini App</b>\n\n"
        f"URL: <code>{esc(webapp_url(shop_id()) if WEBAPP_URL else '—')}</code>\n"
        f"PORT: <code>{PORT}</code>\n"
        f"الفحص الخارجي: {esc(chk)}\n"
        f"سكربت تيليجرام (tg.js): {tg_ok}\n"
        f"الخادم يعمل: {'✅' if WEB_RUNNER else '❌'}\n"
        f"قاعدة البيانات: {'PostgreSQL' if USE_PG else 'SQLite'} — {esc(db)}"
    )
    await update.message.reply_text(t)


async def global_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    c = update.message.contact
    if not c or c.user_id != update.effective_user.id:
        await update.message.reply_text(T("phone_not_yours"))
        return
    phone = "".join(ch for ch in c.phone_number if ch.isdigit())
    save_phone(update.effective_user.id, phone)
    ok = phone.startswith("213") and len(phone) == 12
    await update.message.reply_text(T("phone_ok" if ok else "phone_not_dz"), reply_markup=ReplyKeyboardRemove())


async def cmd_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = ReplyKeyboardMarkup(
        [[KeyboardButton(T("btn_share_phone"), request_contact=True)]],
        resize_keyboard=True, one_time_keyboard=True,
    )
    await update.message.reply_text(T("phone_ask"), reply_markup=kb)

WEB_HTML = r"""<!doctype html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover,user-scalable=no">
<title>Shop</title>
<script async src="/tg.js" onerror="var s=document.createElement('script');s.src='https://telegram.org/js/telegram-web-app.js';document.head.appendChild(s)"></script>
<style>
:root{
  --bg:var(--tg-theme-bg-color,#fff);--tx:var(--tg-theme-text-color,#17181a);--hint:var(--tg-theme-hint-color,#7d838c);
  --btn:var(--tg-theme-button-color,#2a8cff);--btx:var(--tg-theme-button-text-color,#fff);
  --card:var(--tg-theme-secondary-bg-color,#f1f3f7);--line:rgba(127,127,127,.18);
  --star:#f5a524;--star-bg:rgba(245,165,36,.16);--ton:#0098ea;--ton-bg:rgba(0,152,234,.14);
  --red:#e5484d;--green:#2fb67c;--r-lg:20px;--r-md:14px;--r-sm:10px
}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{margin:0}
body{background:var(--bg);color:var(--tx);font:15px/1.6 system-ui,-apple-system,"Segoe UI",Roboto,"Noto Sans Arabic",Tahoma,sans-serif;padding-bottom:calc(24px + env(safe-area-inset-bottom))}
.wrap{padding:16px 16px 28px;max-width:560px;margin:0 auto}
.wrap.has-bar{padding-bottom:110px}
h1{font-size:26px;line-height:1.25;margin:2px 0 4px;font-weight:800}
h2{font-size:21px;line-height:1.3;margin:0 0 6px;font-weight:800}
.hint{color:var(--hint);font-size:13px;margin:0}
.top{display:flex;align-items:center;gap:12px;margin-bottom:18px}
.logo{width:48px;height:48px;border-radius:16px;background:linear-gradient(135deg,var(--btn),var(--ton));color:#fff;display:grid;place-items:center;font-size:22px;font-weight:800;flex:0 0 auto}
.list{display:flex;flex-direction:column;gap:12px}
.card{background:var(--card);border-radius:var(--r-lg);padding:12px;cursor:pointer;display:flex;align-items:center;gap:14px;transition:transform .12s}
.card:active{transform:scale(.98)}
.card.off{opacity:.6}
.tile{width:60px;height:60px;border-radius:var(--r-md);display:grid;place-items:center;font-size:28px;flex:0 0 auto;background:linear-gradient(135deg,rgba(42,140,255,.2),rgba(0,152,234,.08))}
.tile.dig{background:linear-gradient(135deg,rgba(47,182,124,.24),rgba(47,182,124,.07))}
.tile.lg{width:84px;height:84px;font-size:40px;border-radius:24px}
.info{flex:1;min-width:0}
.name{font-weight:700;font-size:16px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sub{color:var(--hint);font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin:1px 0 8px}
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{border-radius:999px;padding:3px 11px;font-size:13px;font-weight:700;background:var(--bg)}
.chip.s{background:var(--star-bg);color:var(--star)}
.chip.t{background:var(--ton-bg);color:var(--ton)}
.badge{background:var(--red);color:#fff;border-radius:999px;padding:3px 10px;font-size:12px;font-weight:600;flex:0 0 auto}
button{font:inherit;border:0;border-radius:var(--r-md);padding:15px 16px;width:100%;cursor:pointer;margin-top:10px;font-weight:700;transition:transform .12s,opacity .2s}
button:active{transform:scale(.98)}
button.primary{background:var(--btn);color:var(--btx)}
button.star{background:var(--star);color:#2b1d00}
button.tonb{background:var(--ton);color:#fff}
button.secondary{background:var(--card);color:var(--tx)}
button:disabled{opacity:.5}
.bar{position:fixed;inset:auto 0 0 0;padding:12px 16px calc(12px + env(safe-area-inset-bottom));background:var(--bg);border-top:1px solid var(--line);z-index:5}
.bar button{margin:0;max-width:528px;display:block;margin-inline:auto}
.group{background:var(--card);border-radius:var(--r-lg);padding:4px 14px 14px;margin-top:14px}
.group h3{font-size:14px;margin:14px 0 0;font-weight:700;color:var(--hint)}
label{display:block;margin:12px 0 6px;font-weight:600;font-size:14px}
input,select,textarea{width:100%;font:inherit;color:var(--tx);background:var(--bg);border:1.5px solid var(--line);border-radius:var(--r-sm);padding:12px;outline:0;transition:border-color .15s}
input:focus,select:focus,textarea:focus{border-color:var(--btn)}
.err{color:var(--red);margin-top:10px;font-size:14px;font-weight:600}
.ok{background:rgba(47,182,124,.14);color:var(--green);border-radius:var(--r-sm);padding:11px 12px;margin-top:10px;font-weight:600}
.desc{white-space:pre-wrap;margin:14px 0;color:var(--tx);opacity:.9}
.info-row{display:flex;gap:8px;align-items:center;color:var(--hint);font-size:13px;margin-top:8px}
.hero{display:flex;gap:16px;align-items:center;margin-bottom:6px}
.kv{background:var(--bg);border:1.5px dashed var(--line);border-radius:var(--r-sm);padding:10px 12px;word-break:break-all;font-family:ui-monospace,Menlo,monospace;font-size:13px;direction:ltr;text-align:left;cursor:pointer}
.center{display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;min-height:70vh;padding:20px}
.big{font-size:56px}
.check{width:88px;height:88px;border-radius:50%;background:var(--green);display:grid;place-items:center;color:#fff;font-size:44px;margin-bottom:16px;animation:pop .45s cubic-bezier(.2,1.4,.4,1)}
@keyframes pop{from{transform:scale(.3);opacity:0}to{transform:scale(1);opacity:1}}
.sk{background:linear-gradient(90deg,var(--card) 25%,rgba(127,127,127,.16) 50%,var(--card) 75%);background-size:200% 100%;animation:sh 1.2s infinite;border-radius:var(--r-lg);height:84px;margin-bottom:12px}
@keyframes sh{to{background-position:-200% 0}}
.search{margin:0 0 14px;border-radius:999px;padding:12px 16px}
.spin{width:38px;height:38px;border:4px solid var(--line);border-top-color:var(--btn);border-radius:50%;animation:s 1s linear infinite;margin-bottom:14px}
@keyframes s{to{transform:rotate(360deg)}}
#toast{position:fixed;left:50%;bottom:calc(90px + env(safe-area-inset-bottom));transform:translateX(-50%);background:rgba(20,20,20,.92);color:#fff;padding:10px 16px;border-radius:12px;font-size:14px;opacity:0;pointer-events:none;transition:opacity .25s;max-width:90%;text-align:center;z-index:10}
#toast.on{opacity:1}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style>
</head>
<body>
<div id="app"><div class="wrap"><div class="sk" style="height:48px;width:60%"></div><div class="sk"></div><div class="sk"></div><div class="sk"></div></div></div>
<div id="toast"></div>
<script>
let tg = window.Telegram && window.Telegram.WebApp;
const SID = location.pathname.split('/').pop();
const app = document.getElementById('app');

const I18N = {
  ar: {
    empty:'لا توجد منتجات حالياً.', digital:'💾 منتج رقمي — يصلك مباشرة بعد الدفع', stock:'المتوفر',
    sold:'نفدت الكمية', held:'محجوز لزبون آخر حالياً، جرّب بعد قليل.', unavailable:'غير متاح حالياً',
    buy:'🛒 اشتري الآن', wilaya:'الولاية', pick:'اختر ولايتك', address:'العنوان الكامل للتوصيل',
    share_phone:'مشاركة رقم هاتفي', phone_old:'حدّث تيليجرام، أو افتح محادثة البوت واكتب /phone ثم شارك رقمك.',
    need_phone:'شارك رقم هاتفك الجزائري (+213) أولاً.', phone_hint:'للتأكد أنك من الجزائر',
    pay_stars:'الدفع بالنجوم', pay_ton:'الدفع بـ TON', connect:'💎 ربط المحفظة', pay_ton_btn:'ادفع',
    open_tk:'فتح Tonkeeper', check:'🔄 تحقق من الدفع', ton_wait:'لم يصل التحويل بعد، انتظر قليلاً.',
    ton_title:'الدفع بعملة TON', amount:'المبلغ', addr:'العنوان', memo:'التعليق (إلزامي، لا تغيّره)', copied:'تم النسخ',
    ton_hint:'اربط محفظتك واضغط «ادفع»، أو حوّل يدوياً بنفس المبلغ والتعليق. يتأكد البوت تلقائياً.',
    paid_title:'تم الدفع بنجاح!', paid_msg:'وصلتك تفاصيل الطلب (والمنتج الرقمي إن وُجد) في محادثة البوت.',
    wait_title:'جارٍ تأكيد الدفع…', wait_msg:'لحظات من فضلك.', failed:'لم تكتمل العملية.',
    back_shop:'العودة للمتجر', net_err:'تعذّر الاتصال، حاول مجدداً.', cancelled:'تم إلغاء الدفع.',
    open_tg:'افتح المتجر من داخل تيليجرام.', order:'رقم الطلب', ship_dz:'🇩🇿 التوصيل داخل الجزائر فقط', retry:'إعادة المحاولة',
    contact:'بيانات التوصيل', details:'معلومات إضافية', payment:'طريقة الدفع', phys:'منتج مادي', dig:'منتج رقمي'
  },
  en: {
    empty:'No products available right now.', digital:'💾 Digital product — delivered right after payment', stock:'In stock',
    sold:'Sold out', held:'Reserved for another customer right now, try again shortly.', unavailable:'Currently unavailable',
    buy:'🛒 Buy now', wilaya:'Wilaya (province)', pick:'Choose your wilaya', address:'Full delivery address',
    share_phone:'Share my phone number', phone_old:'Update Telegram, or open the bot chat, send /phone and share your number.',
    need_phone:'Share your Algerian (+213) phone number first.', phone_hint:'To confirm you are in Algeria',
    pay_stars:'Pay with Stars', pay_ton:'Pay with TON', connect:'💎 Connect wallet', pay_ton_btn:'Pay',
    open_tk:'Open Tonkeeper', check:'🔄 Check payment', ton_wait:'Transfer not received yet, wait a bit.',
    ton_title:'Pay with TON', amount:'Amount', addr:'Address', memo:'Comment (required, do not change)', copied:'Copied',
    ton_hint:'Connect your wallet and tap “Pay”, or transfer manually with the same amount and comment. The bot confirms automatically.',
    paid_title:'Payment successful!', paid_msg:'Your order details (and the digital product, if any) were sent to the bot chat.',
    wait_title:'Confirming payment…', wait_msg:'Just a moment.', failed:'The operation was not completed.',
    back_shop:'Back to shop', net_err:'Connection failed, please try again.', cancelled:'Payment cancelled.',
    open_tg:'Open the shop from inside Telegram.', order:'Order number', ship_dz:'🇩🇿 Delivery inside Algeria only', retry:'Retry',
    contact:'Delivery details', details:'Extra details', payment:'Payment', phys:'Physical product', dig:'Digital product'
  }
};
let L = 'ar', S = null, tcUI = null, stack = ['home'];
const st = {p:null, form:null, oid:null, ton:null, timer:null};
const t = k => (I18N[L] && I18N[L][k]) || I18N.ar[k] || k;
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function api(path, opts) {
  opts = opts || {};
  const ctl = new AbortController();
  const to = setTimeout(() => ctl.abort(), 20000);
  try {
    const r = await fetch('/api/' + SID + path, {
      method: opts.method || 'GET', body: opts.body, signal: ctl.signal,
      headers: {'X-Init-Data': tg.initData, 'Content-Type': 'application/json'}
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return await r.json();
  } catch (e) {
    throw new Error(e.name === 'AbortError' ? 'timeout' : e.message);
  } finally { clearTimeout(to); }
}
async function apiRetry(path, opts, n) {
  let last;
  for (let i = 0; i < (n || 3); i++) {
    try { return await api(path, opts); }
    catch (e) { last = e; await sleep(600 * (i + 1)); }
  }
  throw last;
}
function toast(m) {
  const el = document.getElementById('toast');
  el.textContent = m; el.classList.add('on');
  clearTimeout(toast.tm); toast.tm = setTimeout(() => el.classList.remove('on'), 2200);
}
function haptic() { try { tg.HapticFeedback.notificationOccurred('success'); } catch (e) {} }

/* ── التنقل ── */
function show(name, replace) { if (replace) stack = [name]; else stack.push(name); render(); }
function back() { if (stack.length > 1) { stack.pop(); render(); } }
function render() {
  clearInterval(st.timer);
  const v = stack[stack.length - 1];
  try { views[v](); }
  catch (e) { app.innerHTML = '<div class="center"><p>' + t('net_err') + '</p><button class="primary" onclick="location.reload()">' + t('retry') + '</button></div>'; }
  try {
    if (stack.length > 1 && ['wait','done','failed'].indexOf(v) < 0) tg.BackButton.show(); else tg.BackButton.hide();
  } catch (e) {}
  window.scrollTo(0, 0);
}
async function reload() { try { await load(); } catch (e) {} stack = ['home']; render(); }

function chips(p) {
  let h = '';
  if (p.price != null) h += '<span class="chip s">⭐ ' + esc(p.price) + '</span>';
  if (p.price_ton != null) h += '<span class="chip t">💎 ' + esc(p.price_ton) + ' TON</span>';
  return '<div class="chips">' + h + '</div>';
}
function tile(p, lg) { return '<div class="tile' + (p.shipping ? '' : ' dig') + (lg ? ' lg' : '') + '">' + (p.shipping ? '🛍' : '💾') + '</div>'; }

/* ── الصفحات ── */
function vHome() {
  const cards = S.products.map(p => {
    const off = p.state !== 'available';
    const bad = off ? '<span class="badge">' + t('sold') + '</span>' : '';
    const sub = p.description ? esc(p.description.replace(/\s+/g, ' ')) : (p.shipping ? t('phys') : t('dig'));
    return '<div class="card' + (off ? ' off' : '') + '" data-n="' + esc(p.name.toLowerCase()) + '" onclick="openProduct(' + p.id + ')">' +
      tile(p) + '<div class="info"><div class="name">' + esc(p.name) + '</div><div class="sub">' + sub + '</div>' + chips(p) + '</div>' + bad + '</div>';
  }).join('');
  const sb = S.products.length >= 6 ? '<input class="search" type="search" placeholder="🔍" oninput="filt(this.value)">' : '';
  const ini = esc(Array.from(S.title || 'S')[0].toUpperCase());
  app.innerHTML = '<div class="wrap"><div class="top"><div class="logo">' + ini + '</div><div><h1>' + esc(S.title) + '</h1></div></div>' + sb +
    (cards ? '<div class="list" id="grid">' + cards + '</div>' : '<p class="hint">' + t('empty') + '</p>') + '</div>';
}

function filt(q) {
  q = q.trim().toLowerCase();
  document.querySelectorAll('#grid .card').forEach(c => { c.style.display = !q || c.dataset.n.indexOf(q) >= 0 ? '' : 'none'; });
}
function openProduct(id) { st.p = S.products.find(x => x.id === id); if (st.p) show('product'); }

function vProduct() {
  const p = st.p;
  const ok = p.state === 'available' && p.methods.length;
  const note = p.state === 'sold' ? t('sold') : (p.state === 'held' ? t('held') : t('unavailable'));
  const line = p.shipping ? ((p.stock >= 0 ? t('stock') + ': ' + p.stock + ' — ' : '') + t('ship_dz')) : t('digital');
  app.innerHTML = '<div class="wrap' + (ok ? ' has-bar' : '') + '"><div class="hero">' + tile(p, true) +
    '<div class="info"><h2>' + esc(p.name) + '</h2>' + chips(p) + '</div></div>' +
    (p.description ? '<div class="desc">' + esc(p.description) + '</div>' : '') +
    '<div class="info-row">' + line + '</div>' +
    (ok ? '' : '<p class="err">' + note + '</p>') + '</div>' +
    (ok ? '<div class="bar"><button class="primary" onclick="toCheckout()">' + t('buy') + '</button></div>' : '');
}

function toCheckout() {
  st.form = {wilaya: '', address: '', extras: st.p.extras.map(() => '')};
  show('checkout');
}

function phoneHtml() {
  if (S.phone) return '<div class="ok">📱 +' + esc(S.phone) + ' ✅</div>';
  return '<p class="hint" style="margin-top:12px">🇩🇿 ' + t('phone_hint') + '</p><button class="secondary" style="background:var(--bg)" onclick="askPhone()">📱 ' + t('share_phone') + '</button>';
}
function askPhone() {
  if (!tg.requestContact) { tg.showAlert(t('phone_old')); return; }
  tg.requestContact(() => pollPhone(0));
}
async function pollPhone(n) {
  try {
    const r = await api('/phone');
    if (r.phone) { S.phone = r.phone; const b = document.getElementById('phoneBox'); if (b) b.innerHTML = phoneHtml(); return; }
  } catch (e) {}
  if (n < 20) setTimeout(() => pollPhone(n + 1), 1500);
}

function vCheckout() {
  const p = st.p, f = st.form;
  if (p.methods.indexOf('ton') >= 0) loadTC();
  let h = '<div class="wrap"><div class="hero">' + tile(p) + '<div class="info"><h2 style="font-size:18px">' + esc(p.name) + '</h2>' + chips(p) + '</div></div>';
  if (p.shipping) {
    h += '<div class="group"><h3>' + t('contact') + '</h3><div id="phoneBox">' + phoneHtml() + '</div>';
    h += '<label>' + t('wilaya') + '</label><select onchange="st.form.wilaya=this.value"><option value="">' + t('pick') + '</option>' +
      S.wilayas.map((w, i) => '<option value="' + (i + 1) + '"' + (String(f.wilaya) === String(i + 1) ? ' selected' : '') + '>' +
        String(i + 1).padStart(2, '0') + ' ' + esc(w) + '</option>').join('') + '</select>';
    h += '<label>' + t('address') + '</label><textarea rows="3" oninput="st.form.address=this.value">' + esc(f.address) + '</textarea></div>';
  } else {
    h += '<div class="ok" style="margin-top:14px">' + t('digital') + '</div>';
  }
  if (p.extras.length) {
    h += '<div class="group"><h3>' + t('details') + '</h3>';
    p.extras.forEach((x, i) => {
      h += '<label>' + esc(x.label) + '</label><input data-i="' + i + '"' + (x.kind === 'number' ? ' inputmode="numeric"' : '') +
        ' value="' + esc(f.extras[i]) + '" oninput="st.form.extras[' + i + ']=this.value">' +
        (x.rules.length ? '<div class="hint" style="margin-top:4px">' + x.rules.map(esc).join(' · ') + '</div>' : '');
    });
    h += '</div>';
  }
  h += '<div id="err" class="err"></div><div id="payBtns" style="margin-top:6px">';
  p.methods.forEach(m => {
    h += '<button class="' + (m === 'stars' ? 'star' : 'tonb') + '" onclick="pay(\'' + m + '\')">' +
      (m === 'stars' ? '⭐ ' + t('pay_stars') + ' (' + esc(p.price) + ')' : '💎 ' + t('pay_ton') + ' (' + esc(p.price_ton) + ' TON)') + '</button>';
  });
  app.innerHTML = h + '</div></div>';
}

function lock(on) { document.querySelectorAll('#payBtns button').forEach(b => b.disabled = on); }
function errText(r) { return r.code === 'phone' ? t('need_phone') : (r.code === 'server' ? t('net_err') : (r.error || t('net_err'))); }

async function pay(method) {
  const p = st.p, f = st.form, err = document.getElementById('err');
  err.textContent = ''; lock(true);
  try {
    const r = await api('/order', {method: 'POST', body: JSON.stringify(
      {pid: p.id, method: method, wilaya: f.wilaya, address: f.address, extras: f.extras})});
    if (!r.ok) {
      err.textContent = errText(r); lock(false);
      if (r.code === 'extra') { const el = document.querySelector('[data-i="' + r.index + '"]'); if (el) el.focus(); }
      return;
    }
    st.oid = r.oid;
    if (method === 'stars') {
      tg.openInvoice(r.invoice, s => {
        if (s === 'paid' || s === 'pending') waitPaid();
        else { lock(false); if (s === 'cancelled') toast(t('cancelled')); else err.textContent = t('failed'); }
      });
    } else {
      st.ton = r.ton; show('ton'); startPoll();
    }
  } catch (e) { err.textContent = t('net_err'); lock(false); }
}

function startPoll() {
  clearInterval(st.timer);
  let n = 0;
  st.timer = setInterval(async () => {
    n++;
    try {
      const r = await api('/order/' + st.oid);
      if (r.status === 'paid' || r.status === 'shipped') { clearInterval(st.timer); haptic(); show('done', true); }
      else if (r.status === 'refunded' || r.status === 'cancelled') { clearInterval(st.timer); show('failed', true); }
    } catch (e) {}
    if (n > 180) clearInterval(st.timer);
  }, 2500);
}
function waitPaid() { show('wait', true); startPoll(); }

/* ── TON داخل التطبيق (TON Connect) ── */
function vTon() {
  const x = st.ton;
  app.innerHTML = '<div class="wrap"><h2>💎 ' + t('ton_title') + '</h2><p class="hint">' + t('ton_hint') + '</p>' +
    '<div class="group"><label>' + t('amount') + '</label><div class="kv" onclick="cp(\'' + esc(x.amount) + '\')">' + esc(x.amount) + ' TON</div>' +
    '<label>' + t('addr') + '</label><div class="kv" onclick="cp(\'' + esc(x.address) + '\')">' + esc(x.address) + '</div>' +
    '<label>' + t('memo') + '</label><div class="kv" onclick="cp(\'' + esc(x.memo) + '\')">' + esc(x.memo) + '</div></div>' +
    '<button class="tonb" id="tonBtn" onclick="tonPay()">' + t('connect') + '</button>' +
    '<button class="secondary" onclick="tg.openLink(st.ton.url)">' + t('open_tk') + '</button>' +
    '<button class="secondary" onclick="checkTon()">' + t('check') + '</button></div>';
  setupTC();
}
function loadTC() {
  return new Promise(res => {
    if (window.TON_CONNECT_UI) return res(true);
    if (loadTC.p) return loadTC.p.then(res);
    loadTC.p = new Promise(r2 => {
      const sc = document.createElement('script');
      sc.src = 'https://cdn.jsdelivr.net/npm/@tonconnect/ui@2.0.9/dist/tonconnect-ui.min.js';
      sc.onload = () => r2(true); sc.onerror = () => { loadTC.p = null; r2(false); };
      document.head.appendChild(sc);
    });
    loadTC.p.then(res);
  });
}
async function setupTC() {
  try {
    if (!(await loadTC())) return;
    if (!tcUI) {
      tcUI = new TON_CONNECT_UI.TonConnectUI({
        manifestUrl: location.origin + '/tonconnect-manifest.json',
        actionsConfiguration: {twaReturnUrl: 'https://t.me/' + S.bot}
      });
      tcUI.onStatusChange(() => tonLabel());
    }
    tcUI.connectionRestored.then(tonLabel);
    tonLabel();
  } catch (e) { tcUI = null; }
}
function tonLabel() {
  const b = document.getElementById('tonBtn');
  if (b && tcUI && st.ton) b.textContent = tcUI.connected ? ('💎 ' + t('pay_ton_btn') + ' ' + st.ton.amount + ' TON') : t('connect');
}
async function tonPay() {
  if (!tcUI) { tg.openLink(st.ton.url); return; }
  try {
    if (!tcUI.connected) { await tcUI.openModal(); return; }
    await tcUI.sendTransaction({
      validUntil: Math.floor(Date.now() / 1000) + 600,
      messages: [{address: st.ton.address, amount: st.ton.nano, payload: st.ton.payload}]
    });
    toast(t('wait_title'));
  } catch (e) {}
}
async function checkTon() {
  try {
    const r = await api('/order/' + st.oid + '/check', {method: 'POST'});
    if (r.status === 'paid' || r.status === 'shipped') { haptic(); show('done', true); } else toast(t('ton_wait'));
  } catch (e) { toast(t('net_err')); }
}
function cp(x) { try { navigator.clipboard.writeText(x).then(() => toast(t('copied'))); } catch (e) {} }

function vWait() {
  app.innerHTML = '<div class="center"><div class="spin"></div><h2>' + t('wait_title') + '</h2><p class="hint">' + t('wait_msg') + '</p></div>';
}
function vDone() {
  app.innerHTML = '<div class="center"><div class="check">✓</div><h2>' + t('paid_title') + '</h2><p class="hint">' + t('order') +
    ' #' + esc(st.oid) + '<br>' + t('paid_msg') + '</p><button class="primary" onclick="reload()">' + t('back_shop') + '</button></div>';
}
function vFailed() {
  app.innerHTML = '<div class="center"><div class="big">❌</div><h2>' + t('failed') + '</h2>' +
    '<button class="primary" onclick="reload()">' + t('back_shop') + '</button></div>';
}
const views = {home: vHome, product: vProduct, checkout: vCheckout, ton: vTon, wait: vWait, done: vDone, failed: vFailed};

async function load() {
  const r = await apiRetry('/home', null, 3);
  if (!r || r.ok === false || !Array.isArray(r.products)) throw new Error('server');
  S = r;
  applyLang();
}
function applyLang() {
  L = S.lang || 'ar';
  document.documentElement.lang = L;
  document.documentElement.dir = L === 'ar' ? 'rtl' : 'ltr';
}
(async () => {
  for (let i = 0; i < 40 && !(window.Telegram && window.Telegram.WebApp && window.Telegram.WebApp.initData); i++) await sleep(150);
  tg = window.Telegram && window.Telegram.WebApp;
  if (!tg || !tg.initData) {
    app.innerHTML = '<div class="center"><p>' + I18N.ar.open_tg + '<br>' + I18N.en.open_tg + '</p></div>';
    return;
  }
  try { tg.ready(); tg.expand(); } catch (e) {}
  try { if (tg.BackButton) tg.BackButton.onClick(back); } catch (e) {}
  try {
    await load();
    render();
  } catch (e) {
    app.innerHTML = '<div class="center"><p>' + t('net_err') + '<br><small dir="ltr">' + esc(e.message) +
      '</small></p><button class="primary" onclick="location.reload()">' + t('retry') + '</button></div>';
  }
})();
</script>
</body>
</html>
"""


def admin_commands(lang, is_main):
    cmds = [
        BotCommand("addproduct", T("cmd_addproduct", lang)),
        BotCommand("products", T("cmd_products", lang)),
        BotCommand("orders", T("cmd_orders", lang)),
        BotCommand("shipped", T("cmd_shipped", lang)),
        BotCommand("refund", T("cmd_refund", lang)),
        BotCommand("setwallet", T("cmd_setwallet", lang)),
        BotCommand("diag", T("cmd_diag", lang)),
        BotCommand("help", T("cmd_help", lang)),
        BotCommand("lang", T("cmd_lang", lang)),
        BotCommand("start", T("cmd_start", lang)),
    ]
    if is_main:
        cmds += [
            BotCommand("bots", T("cmd_bots", lang)),
            BotCommand("delbot", T("cmd_delbot", lang)),
        ]
    return cmds


def user_commands(lang):
    return [BotCommand("start", T("cmd_start", lang))]


async def set_admin_commands(bot, admin_id, sid):
    if not admin_id:
        return
    try:
        lang = get_shop_lang(sid)
        await bot.set_my_commands(admin_commands(lang, sid == 0), scope=BotCommandScopeChat(admin_id))
    except Exception as e:
        log.warning("admin set_my_commands failed: %s", e)


async def setup_commands(bot, admin_id, sid):
    try:
        lang = get_shop_lang(sid)
        cmds = user_commands(lang)
        await bot.set_my_commands(cmds, scope=BotCommandScopeDefault())
        for lg in LANGS:
            await bot.set_my_commands(cmds, scope=BotCommandScopeDefault(), language_code=lg)
    except Exception as e:
        log.warning("default set_my_commands failed: %s", e)
    if WEBAPP_URL:
        try:
            await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(
                text=T("menu_btn", get_shop_lang(sid)), web_app=WebAppInfo(url=webapp_url(sid))))
        except Exception as e:
            log.warning("set_chat_menu_button failed: %s", e)
    await set_admin_commands(bot, admin_id, sid)


async def bind_shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    shop = context.application.bot_data["shop"]
    _shop.set(shop)
    _lang.set(get_shop_lang(shop["id"]))


async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled error", exc_info=context.error)


async def post_init(app: Application):
    global MAIN_APP
    MAIN_APP = app
    await start_web()
    await setup_commands(app.bot, ADMIN_ID, 0)
    await load_shops()
    if not ADMIN_ID:
        return
    L = get_shop_lang(0)
    msg = T("su_ok", L)
    if USE_PG:
        msg += T("su_pg", L)
    else:
        msg += T("su_sqlite", L, path=os.path.abspath(DB_PATH))
        if not os.path.abspath(DB_PATH).startswith("/data"):
            msg += T("su_warn", L)
    if RUNNING:
        msg += T("su_subs", L, n=len(RUNNING))
    if WEBAPP_URL:
        msg += f"\n🌐 Mini App: {webapp_url(0)}\n{await self_check()}"
    msg += T("su_help", L)
    try:
        await app.bot.send_message(ADMIN_ID, msg)
    except Exception as e:
        log.warning("startup notice failed: %s", e)


async def post_shutdown(app: Application):
    await stop_web()
    for bid in list(RUNNING):
        await stop_shop(bid)
    try:
        if _TON_HTTP is not None and not _TON_HTTP.is_closed:
            await _TON_HTTP.aclose()
    except Exception:
        pass


def register_handlers(app: Application, is_main: bool):
    text_only = filters.TEXT & ~filters.COMMAND

    app.add_handler(TypeHandler(Update, bind_shop), group=-1)

    admin_conv = ConversationHandler(
        entry_points=[
            CommandHandler("addproduct", cmd_addproduct),
            CommandHandler("setwallet", cmd_setwallet),
            CallbackQueryHandler(wallet_entry, pattern=r"^adm_wallet$"),
            CallbackQueryHandler(new_product_cb, pattern=r"^adm_new$"),
            CallbackQueryHandler(edit_entry, pattern=r"^edt_(name|desc|price|ton|stock|photos|deliv)_\d+$"),
            CallbackQueryHandler(x_entry, pattern=r"^(xnew_\d+|xedit_\d+_\d+)$"),
            CallbackQueryHandler(mup_entry, pattern=r"^mup_(l|x)$"),
        ],
        states={
            A_NAME: [MessageHandler(text_only, got_name)],
            A_PRICE: [MessageHandler(text_only, got_price)],
            A_TYPE: [CallbackQueryHandler(got_type, pattern=r"^nt_(ship|dig)$")],
            A_STOCK: [MessageHandler(text_only, got_stock)],
            A_METHOD: [CallbackQueryHandler(got_method, pattern=r"^pm_(stars|ton|both)$")],
            A_TON: [MessageHandler(text_only, got_ton)],
            W_ADDR: [MessageHandler(text_only, got_wallet)],
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
            X_LABEL: [MessageHandler(text_only, got_x_label)],
            X_TYPE: [CallbackQueryHandler(got_x_type, pattern=r"^xt_(num|text)$")],
            X_LEN: [MessageHandler(text_only, got_x_len)],
            X_PRE: [MessageHandler(text_only, got_x_pre)],
            X_SUF: [MessageHandler(text_only, got_x_suf)],
            M_UP: [
                MessageHandler(filters.PHOTO | filters.VIDEO, got_lmedia),
                CommandHandler(["done", "skip"], lmedia_done),
                MessageHandler(text_only, lmedia_wrong),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", adm_cancel),
            CommandHandler("start", start),
        ],
        allow_reentry=True,
    )

    order_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(buy_cb, pattern=r"^buy_\d+$")],
        states={
            PHONE: [
                MessageHandler(filters.CONTACT, got_phone),
                MessageHandler(text_only, phone_text),
            ],
            WILAYA: [CallbackQueryHandler(got_wilaya, pattern=r"^w_\d+$")],
            ADDRESS: [MessageHandler(text_only, got_address)],
            EXTRA: [MessageHandler(text_only, got_extra)],
            CONFIRM: [
                CallbackQueryHandler(pay_cb, pattern="^pay$"),
                CallbackQueryHandler(payton_cb, pattern="^payton$"),
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
    app.add_handler(order_conv)
    app.add_handler(MessageHandler(filters.CONTACT, global_contact))
    app.add_handler(CommandHandler("phone", cmd_phone))

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(prod_cb, pattern=r"^prod_\d+$"))
    app.add_handler(CallbackQueryHandler(list_cb, pattern=r"^list$"))

    app.add_handler(PreCheckoutQueryHandler(pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, on_paid))
    app.add_handler(CallbackQueryHandler(ton_check_cb, pattern=r"^tonchk_\d+$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(PAID_RE), user_says_paid))

    app.add_handler(CommandHandler("lang", cmd_lang))
    app.add_handler(CommandHandler("diag", cmd_diag))
    app.add_handler(CallbackQueryHandler(lang_cb, pattern=r"^setlang_(ar|en)$"))
    app.add_handler(CallbackQueryHandler(adm_list_cb, pattern=r"^adm_list$"))
    app.add_handler(CallbackQueryHandler(adm_cb, pattern=r"^adm_(panel|ship|active|offstars|offton|clrphotos|del|delyes)_\d+$"))
    app.add_handler(CallbackQueryHandler(x_cb, pattern=r"^(xl_\d+|xpick_\d+|xat_\d+_\d+|xf_\d+_\d+|xun_\d+_\d+|xkill_\d+_\d+)$"))
    app.add_handler(CallbackQueryHandler(
        media_cb, pattern=r"^(lm_open|lm_clear|mpick_(p\d+|l|x)_\d+|madd_(p\d+|l)_\d+|mdel_\d+_\d+)$"))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("products", cmd_products))
    app.add_handler(CommandHandler("orders", cmd_orders))
    app.add_handler(CommandHandler("shipped", cmd_shipped))
    app.add_handler(CommandHandler("refund", cmd_refund))

    if is_main:
        app.add_handler(CommandHandler("bots", cmd_bots))
        app.add_handler(CommandHandler("delbot", cmd_delbot))
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


def main():
    init_db()
    app = build_app(BOT_TOKEN, {"id": 0, "admin": ADMIN_ID, "username": None}, is_main=True)
    log.info("Bot started (polling)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
