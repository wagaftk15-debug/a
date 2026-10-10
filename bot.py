import os
import re
import html
import time
import asyncio
import sqlite3
import logging
import threading
from contextlib import contextmanager
from contextvars import ContextVar

import httpx
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

# ── TON ──
TON_WALLET = os.environ.get("TON_WALLET", "")          # محفظة البوت الرئيسي (اختياري، أو استعمل /setwallet)
TONCENTER_KEY = os.environ.get("TONCENTER_KEY", "")    # مفتاح مجاني من @tonapibot (اختياري لكن يُنصح به)
WALLET_RE = re.compile(r"^(EQ|UQ)[A-Za-z0-9_-]{46}$")

RESERVE_MINUTES = 10
MAX_PHOTOS = 10
DB_PATH = os.environ.get("DB_PATH", "shop.db")

# اللغة الافتراضية لمن لم يختر لغة بعد: ar أو en
LANGS = ("ar", "en")
DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "ar").lower()
if DEFAULT_LANG not in LANGS:
    DEFAULT_LANG = "ar"

# حالات طلب الزبون
PHONE, WILAYA, ADDRESS, CONFIRM, EXTRA = range(5)
# حالات الأدمن
A_NAME, A_PRICE, A_TYPE, A_STOCK, A_DESC, A_PHOTOS, A_DELIV, E_VALUE, X_LABEL, X_TYPE = range(20, 30)

WILAYAS = [
    "أدرار", "الشلف", "الأغواط", "أم البواقي", "باتنة", "بجاية", "بسكرة", "بشار", "البليدة", "البويرة",
    "تمنراست", "تبسة", "تلمسان", "تيارت", "تيزي وزو", "الجزائر", "الجلفة", "جيجل", "سطيف", "سعيدة",
    "سكيكدة", "سيدي بلعباس", "عنابة", "قالمة", "قسنطينة", "المدية", "مستغانم", "المسيلة", "معسكر", "ورقلة",
    "وهران", "البيض", "إليزي", "برج بوعريريج", "بومرداس", "الطارف", "تندوف", "تيسمسيلت", "الوادي", "خنشلة",
    "سوق أهراس", "تيبازة", "ميلة", "عين الدفلى", "النعامة", "عين تموشنت", "غرداية", "غليزان", "تيميمون",
    "برج باجي مختار", "أولاد جلال", "بني عباس", "عين صالح", "عين قزام", "تقرت", "جانت", "المغير", "المنيعة",
]

WILAYAS_EN = [
    "Adrar", "Chlef", "Laghouat", "Oum El Bouaghi", "Batna", "Béjaïa", "Biskra", "Béchar", "Blida", "Bouira",
    "Tamanrasset", "Tébessa", "Tlemcen", "Tiaret", "Tizi Ouzou", "Algiers", "Djelfa", "Jijel", "Sétif", "Saïda",
    "Skikda", "Sidi Bel Abbès", "Annaba", "Guelma", "Constantine", "Médéa", "Mostaganem", "M'Sila", "Mascara", "Ouargla",
    "Oran", "El Bayadh", "Illizi", "Bordj Bou Arréridj", "Boumerdès", "El Tarf", "Tindouf", "Tissemsilt", "El Oued", "Khenchela",
    "Souk Ahras", "Tipaza", "Mila", "Aïn Defla", "Naâma", "Aïn Témouchent", "Ghardaïa", "Relizane", "Timimoun",
    "Bordj Badji Mokhtar", "Ouled Djellal", "Béni Abbès", "In Salah", "In Guezzam", "Touggourt", "Djanet", "El M'Ghair", "El Menia",
]

# ───────────────────────── النصوص (عربي / English) ─────────────────────────
# (المفتاح، العربية، English)
_S = [
    # عام
    ("no_products", "🚧 لا توجد منتجات حالياً.", "🚧 No products available right now."),
    ("admin_add_hint", "\n\nأنت الأدمن: اكتب /addproduct لإضافة أول منتج.",
     "\n\nYou are the admin: send /addproduct to add your first product."),
    ("lang_pick", "🌐 اختر لغة المتجر (تُطبَّق على الجميع):\nChoose the shop language (applies to everyone):",
     "🌐 اختر لغة المتجر (تُطبَّق على الجميع):\nChoose the shop language (applies to everyone):"),
    ("lang_set", "✅ تم تغيير لغة المتجر إلى العربية للجميع.", "✅ Shop language changed to English for everyone."),
    ("btn_all_products", "🔙 كل المنتجات", "🔙 All products"),
    ("list_title", "🛍 <b>منتجاتنا</b>\n\nاختر منتجاً لعرض تفاصيله 👇",
     "🛍 <b>Our products</b>\n\nPick a product to see its details 👇"),
    ("product_gone", "هذا المنتج لم يعد متوفراً.", "This product is no longer available."),
    ("price_line", "💰 السعر: <b>{price} ⭐</b> (نجوم تيليجرام)\n", "💰 Price: <b>{price} ⭐</b> (Telegram Stars)\n"),
    ("price_ton_line", "💎 أو: <b>{ton} TON</b>\n", "💎 Or: <b>{ton} TON</b>\n"),
    ("stock_line", "📦 المتوفر: <b>{n}</b>\n", "📦 In stock: <b>{n}</b>\n"),
    ("dz_only", "🇩🇿 البيع والتوصيل داخل الجزائر فقط\n", "🇩🇿 Sales and delivery inside Algeria only\n"),
    ("digital_note", "💾 منتج رقمي — يصلك مباشرة بعد الدفع\n", "💾 Digital product — delivered right after payment\n"),
    ("buy_prompt", "اضغط الزر أدناه لإتمام الطلب 👇", "Tap the button below to order 👇"),
    ("btn_buy", "🛒 اشتري الآن", "🛒 Buy now"),
    ("unavail_sold", "❌ للأسف، نفدت الكمية.", "❌ Sorry, this item is sold out."),
    ("unavail_held", "⏳ القطعة محجوزة حالياً لزبون آخر (يدفع الآن). جرّب بعد عدة دقائق، فقد تعود متاحة.",
     "⏳ This item is currently reserved for another customer (paying now). Try again in a few minutes; it may become available."),
    ("unlimited", "غير محدود", "Unlimited"),
    # خطوات الطلب
    ("btn_cancel", "❌ إلغاء", "❌ Cancel"),
    ("btn_pay", "💳 تأكيد والدفع {price}⭐", "💳 Confirm & pay {price}⭐"),
    ("btn_edit_addr", "✏️ تعديل الولاية/العنوان", "✏️ Edit wilaya/address"),
    ("summary_title", "📋 <b>راجع طلبك:</b>\n\n🛍 {name}\n", "📋 <b>Review your order:</b>\n\n🛍 {name}\n"),
    ("summary_amount", "\n💰 المبلغ: <b>{price}⭐</b>", "\n💰 Amount: <b>{price}⭐</b>"),
    ("summary_amount_ton", " / <b>{ton} TON</b>", " / <b>{ton} TON</b>"),
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
    ("session_expired", "انتهت الجلسة، اكتب /start للبدء من جديد.", "Session expired. Send /start to begin again."),
    ("reserved", "⏳ تم حجز طلبك لمدة {m} دقائق. أكمل الدفع من الفاتورة أدناه 👇",
     "⏳ Your order is reserved for {m} minutes. Complete the payment using the invoice below 👇"),
    ("invoice_ship_desc", "{name} — توصيل داخل الجزائر", "{name} — delivery inside Algeria"),
    ("cancelled", "تم إلغاء الطلب. اكتب /start للبدء من جديد.", "Order cancelled. Send /start to begin again."),
    # الدفع
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
    # TON
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
    ("help_ton", "\n/setwallet عنوان — محفظة TON لاستلام الدفع", "\n/setwallet address — TON wallet to receive payments"),
    ("cmd_setwallet", "💎 محفظة TON", "💎 TON wallet"),
    # إشعارات الأدمن
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
    # لوحة المنتجات
    ("not_found", "المنتج غير موجود.", "Product not found."),
    ("pt_type_ship", "النوع: 🚚 مادي (شحن + ولايات)", "Type: 🚚 Physical (shipping + wilayas)"),
    ("pt_type_dig", "النوع: 💾 رقمي (بدون شحن)", "Type: 💾 Digital (no shipping)"),
    ("pt_price", "💰 السعر: {p}⭐", "💰 Price: {p}⭐"),
    ("pt_stock", "📦 المخزون: {s}", "📦 Stock: {s}"),
    ("pt_media", "📷 الوسائط (صور/فيديو): {n}", "📷 Media (photos/videos): {n}"),
    ("pt_visible", "الحالة: ✅ ظاهر للزبائن", "Status: ✅ Visible to customers"),
    ("pt_hidden", "الحالة: 🚫 مخفي", "Status: 🚫 Hidden"),
    ("pt_extra", "🧩 حقل الزبون: {label} ({kind})", "🧩 Customer field: {label} ({kind})"),
    ("kind_num", "🔢 رقم", "🔢 Number"),
    ("kind_text", "🔤 نص", "🔤 Text"),
    ("pt_deliv", "🎁 محتوى التسليم: نص {a} | ملف {b}", "🎁 Delivery content: text {a} | file {b}"),
    ("b_name", "✏️ الاسم", "✏️ Name"),
    ("b_desc", "📝 الوصف", "📝 Description"),
    ("b_price", "💰 السعر", "💰 Price"),
    ("b_stock", "📊 المخزون", "📊 Stock"),
    ("b_photos", "📷 تغيير الصور/الفيديو", "📷 Change photos/videos"),
    ("b_clr", "🧹 مسح الوسائط", "🧹 Clear media"),
    ("b_to_dig", "🔁 تحويل إلى رقمي", "🔁 Switch to digital"),
    ("b_to_phys", "🔁 تحويل إلى مادي (شحن)", "🔁 Switch to physical (shipping)"),
    ("b_deliv", "🎁 محتوى التسليم", "🎁 Delivery content"),
    ("b_extra", "🧩 حقل يرسله الزبون", "🧩 Customer input field"),
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
    # إضافة منتج
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
     "📷 أرسل <b>صور و/أو فيديوهات المنتج</b> (حتى {n} في المجموع، كصور/فيديو وليس كملفات).\nعند الانتهاء اكتب /done — أو /skip إذا لا تريد وسائط.",
     "📷 Send the <b>product photos and/or videos</b> (up to {n} in total, as photos/videos, not files).\nWhen finished send /done — or /skip for no media."),
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
    # التعديل
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
    # حقل الزبون
    ("x_ask",
     "🧩 اكتب <b>السؤال/الطلب الذي سيظهر للزبون</b> قبل الدفع.\nمثال: <i>أرسل رقم حسابك (ID)</i> أو <i>اكتب اسم اللاعب</i>\n\nأرسل <b>-</b> لحذف الحقل.\n(للإلغاء: /cancel)",
     "🧩 Type the <b>question/request shown to the customer</b> before payment.\nExample: <i>Send your account ID</i> or <i>Type the player name</i>\n\nSend <b>-</b> to remove the field.\n(to cancel: /cancel)"),
    ("x_removed", "🗑 تم حذف الحقل.", "🗑 Field removed."),
    ("x_bad_len", "⚠️ اكتب نصاً من 2 إلى 200 حرف.", "⚠️ Enter text of 2 to 200 characters."),
    ("b_x_num", "🔢 رقم فقط", "🔢 Numbers only"),
    ("b_x_text", "🔤 نص حر", "🔤 Free text"),
    ("x_type_ask", "ما <b>نوع الإجابة</b> المطلوبة من الزبون؟", "What <b>type of answer</b> do you want from the customer?"),
    ("x_saved", "✅ تم حفظ الحقل. سيُطلب من الزبون قبل الدفع.", "✅ Field saved. It will be requested from the customer before payment."),
    # الطلبات
    ("no_orders", "لا توجد طلبات مدفوعة بعد.", "No paid orders yet."),
    ("shipped_usage", "الاستعمال: <code>/shipped رقم_الطلب</code>", "Usage: <code>/shipped order_number</code>"),
    ("shipped_bad", "الطلب غير موجود أو غير مدفوع.", "Order not found or not paid."),
    ("shipped_ok", "✅ الطلب #{id} أصبح «تم الشحن».", "✅ Order #{id} marked as shipped."),
    ("refund_usage", "الاستعمال: <code>/refund رقم_الطلب</code>", "Usage: <code>/refund order_number</code>"),
    ("refund_bad", "لا يمكن استرجاع هذا الطلب.", "This order cannot be refunded."),
    ("refund_fail", "فشل الاسترجاع: {err}", "Refund failed: {err}"),
    ("refund_ok", "💸 تم استرجاع {amount}⭐ للطلب #{id}.", "💸 Refunded {amount}⭐ for order #{id}."),
    ("help_admin",
     "🛠 <b>أوامر الأدمن</b>\n\n/addproduct — إضافة منتج جديد\n/products — إدارة المنتجات (تعديل، صور، شحن/رقمي، مخزون، حقل الزبون، سعر TON، إخفاء، حذف)\n/orders — الطلبات المدفوعة\n/shipped رقم — تم الشحن\n/refund رقم — استرجاع النجوم\n/lang — تغيير لغة المتجر للجميع",
     "🛠 <b>Admin commands</b>\n\n/addproduct — add a new product\n/products — manage products (edit, media, physical/digital, stock, customer field, TON price, hide, delete)\n/orders — paid orders\n/shipped number — mark as shipped\n/refund number — refund the stars\n/lang — change the shop language (for everyone)"),
    ("help_multi",
     "\n\n🤖 <b>البوتات المتعددة</b>\nأرسل هنا <b>توكن بوت جديد</b> (من @BotFather) لربطه وتشغيله فوراً.\n/bots — البوتات المرتبطة\n/delbot ID — فصل بوت وإيقافه",
     "\n\n🤖 <b>Multiple bots</b>\nSend a <b>new bot token</b> (from @BotFather) here to link and start it right away.\n/bots — linked bots\n/delbot ID — unlink and stop a bot"),
    # البوتات المتعددة
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
    # الأوامر (القائمة)
    ("cmd_start", "🏠 الصفحة الرئيسية", "🏠 Home"),
    ("cmd_lang", "🌐 لغة المتجر للجميع", "🌐 Shop language (for everyone)"),
    ("cmd_addproduct", "➕ إضافة منتج", "➕ Add product"),
    ("cmd_products", "🗂 إدارة المنتجات", "🗂 Manage products"),
    ("cmd_orders", "📦 الطلبات المدفوعة", "📦 Paid orders"),
    ("cmd_shipped", "🚚 تم الشحن (رقم)", "🚚 Mark shipped (number)"),
    ("cmd_refund", "💸 استرجاع (رقم)", "💸 Refund (number)"),
    ("cmd_help", "🛠 أوامر الأدمن", "🛠 Admin commands"),
    ("cmd_bots", "🤖 البوتات المرتبطة", "🤖 Linked bots"),
    ("cmd_delbot", "🗑 فصل بوت (ID)", "🗑 Unlink a bot (ID)"),
    # رسالة بدء التشغيل
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

# اللغة الحالية للتحديث الجاري
_lang = ContextVar("lang", default=DEFAULT_LANG)


def T(key, lang=None, **kw):
    """نص مترجم حسب لغة المستخدم الحالي (أو لغة محددة)."""
    lang = lang or _lang.get()
    s = STR.get(lang, STR["ar"]).get(key) or STR["ar"][key]
    return s.format(**kw)


def wilaya_names(lang=None):
    return WILAYAS_EN if (lang or _lang.get()) == "en" else WILAYAS


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
    # الحقل المخصص (يرسله الزبون: رقم أو نص)
    ensure_column("shop_products", "extra_label", "TEXT")
    ensure_column("shop_products", "extra_type", "TEXT")
    ensure_column("shop_orders", "extra_label", "TEXT")
    ensure_column("shop_orders", "extra_value", "TEXT")
    # الدفع بعملة TON
    ensure_column("shop_products", "price_ton", "DOUBLE PRECISION")
    ensure_column("shop_orders", "ton_nano", "BIGINT")

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


# ── لغة المتجر (يحددها الأدمن وتسري على الجميع) ──
_SHOP_LANG = {}


def get_shop_lang(sid):
    if sid in _SHOP_LANG:
        return _SHOP_LANG[sid]
    lang = DEFAULT_LANG
    try:
        with cursor() as cur:
            cur.execute("SELECT value FROM shop_settings WHERE key=?", (f"lang_{sid}",))
            r = cur.fetchone()
        if r and r["value"] in LANGS:
            lang = r["value"]
    except Exception as e:
        log.warning("get_shop_lang failed: %s", e)
    _SHOP_LANG[sid] = lang
    return lang


def set_shop_lang(sid, lang):
    with cursor() as cur:
        cur.execute(
            "INSERT INTO shop_settings (key, value) VALUES (?,?) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value",
            (f"lang_{sid}", lang),
        )
    _SHOP_LANG[sid] = lang


# ── محفظة TON (لكل متجر محفظته) ──
def get_wallet():
    sid = shop_id()
    try:
        with cursor() as cur:
            cur.execute("SELECT value FROM shop_settings WHERE key=?", (f"ton_wallet_{sid}",))
            r = cur.fetchone()
        if r and r["value"]:
            return r["value"]
    except Exception as e:
        log.warning("get_wallet failed: %s", e)
    return TON_WALLET if sid == 0 else ""


def set_wallet(w):
    with cursor() as cur:
        cur.execute(
            "INSERT INTO shop_settings (key, value) VALUES (?,?) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value",
            (f"ton_wallet_{shop_id()}", w),
        )


# ── المنتجات (كلها مقيّدة بالمتجر الحالي) ──
PRODUCT_FIELDS = {"name", "description", "price", "stock", "shipping", "delivery_text", "delivery_file", "active",
                  "extra_label", "extra_type", "price_ton"}


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


def place_order(user, pid, phone, wilaya, address, extra=None):
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
               product_id, product_name, reserved_until, shop_id, extra_label, extra_value)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id
        """, (user.id, user.username or "", user.full_name or "", phone, wilaya, address,
              p["price"], now, pid, p["name"], now + RESERVE_MINUTES * 60, shop_id(),
              p.get("extra_label") if extra else None, extra))
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


# ───────────────────────── نصوص مساعدة ─────────────────────────
def esc(s):
    return html.escape(str(s or ""))


def stock_label(stock):
    return T("unlimited") if stock < 0 else str(stock)


def fmt_ton(v):
    """يعرض مبلغ TON بدون أصفار زائدة."""
    return f"{float(v):.9f}".rstrip("0").rstrip(".")


def order_text(o, lang=None):
    name = o.get("product_name") or PRODUCT_NAME
    uname = f"@{esc(o['username'])}" if o.get("username") else "—"
    t = (
        T("ot_head", lang, id=o["id"], status=esc(o["status"])) + "\n"
        f"🛍 {esc(name)}\n"
        f"👤 {esc(o['full_name'])} ({uname}) — ID: <code>{o['user_id']}</code>\n"
    )
    if o.get("wilaya"):
        t += f"📱 +{esc(o['phone'])}\n📍 {esc(o['wilaya'])}\n🏠 {esc(o['address'])}\n"
    else:
        t += T("ot_digital", lang) + "\n"
    if o.get("extra_value"):
        t += f"🧩 {esc(o.get('extra_label') or '—')}: <code>{esc(o['extra_value'])}</code>\n"
    if str(o.get("charge_id") or "").startswith("ton_"):
        t += f"💎 {fmt_ton((o.get('ton_nano') or 0) / 1e9)} TON\n"
    else:
        t += f"💰 {o['amount']}⭐"
    return t


def wilaya_keyboard():
    names = wilaya_names()
    rows, row = [], []
    for i, name in enumerate(names, start=1):
        row.append(InlineKeyboardButton(f"{i:02d} {name}", callback_data=f"w_{i}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(T("btn_cancel"), callback_data="cancel")])
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
    await message.reply_text(T("list_title"), reply_markup=InlineKeyboardMarkup(rows))


async def show_product(message, p, uid):
    state = availability(p["id"], uid)
    text = f"{'🛠' if p['shipping'] else '💾'} <b>{esc(p['name'])}</b>\n\n"
    if p["description"]:
        text += f"{esc(p['description'])}\n\n"
    text += T("price_line", price=p["price"])
    if p.get("price_ton") and get_wallet():
        text += T("price_ton_line", ton=fmt_ton(p["price_ton"]))
    if p["shipping"]:
        if p["stock"] >= 0:
            text += T("stock_line", n=p["stock"])
        text += T("dz_only")
    else:
        text += T("digital_note")
    text += "\n"

    rows = []
    if state == "available":
        text += T("buy_prompt")
        rows.append([InlineKeyboardButton(T("btn_buy"), callback_data=f"buy_{p['id']}")])
    else:
        text += T("unavail_" + state)
    if len(list_products()) > 1:
        rows.append([InlineKeyboardButton(T("btn_all_products"), callback_data="list")])
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


async def send_home(message, uid):
    prods = list_products()
    if not prods:
        msg = T("no_products")
        if shop_admin() and uid == shop_admin():
            msg += T("admin_add_hint")
        await message.reply_text(msg)
        return
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
def lang_keyboard():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🇩🇿 العربية", callback_data="setlang_ar"),
        InlineKeyboardButton("🇬🇧 English", callback_data="setlang_en"),
    ]])


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
    set_shop_lang(shop_id(), lang)   # تسري على كل زبائن هذا المتجر
    _lang.set(lang)
    await setup_commands(context.bot, shop_admin(), shop_id())
    try:
        await q.message.edit_text(T("lang_set"))
    except Exception:
        await q.message.reply_text(T("lang_set"))
    await send_home(q.message, q.from_user.id)


# ───────────────────────── خطوات الطلب ─────────────────────────
async def send_summary(message, context):
    d = context.user_data
    p = get_product(d["pid"])
    rows = [[InlineKeyboardButton(T("btn_pay", price=p["price"]), callback_data="pay")]]
    if p["shipping"]:
        rows.append([InlineKeyboardButton(T("btn_edit_addr"), callback_data="restart")])
    rows.append([InlineKeyboardButton(T("btn_cancel"), callback_data="cancel")])
    text = T("summary_title", name=esc(p["name"]))
    if p["shipping"]:
        text += f"📱 +{esc(d['phone'])}\n📍 {esc(d['wilaya'])}\n🏠 {esc(d['address'])}\n"
    else:
        text += T("digital_note")
    if p.get("extra_label") and d.get("extra"):
        text += f"🧩 {esc(p['extra_label'])}: <b>{esc(d['extra'])}</b>\n"
    text += T("summary_amount", price=p["price"])
    if p.get("price_ton") and get_wallet():
        text += T("summary_amount_ton", ton=fmt_ton(p["price_ton"]))
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(rows))


async def ask_extra_or_summary(message, context, p):
    """يسأل الزبون عن الحقل المخصص (إن وُجد ولم يُجب عنه) وإلا يعرض الملخص."""
    if p.get("extra_label") and "extra" not in context.user_data:
        hint = T("extra_hint_num") if p.get("extra_type") == "number" else T("extra_hint_text")
        await message.reply_text(f"🧩 {esc(p['extra_label'])}\n{hint}\n\n{T('cancel_hint')}")
        return EXTRA
    await send_summary(message, context)
    return CONFIRM


AR_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")


async def got_extra(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = context.user_data
    p = get_product(d["pid"]) if d.get("pid") else None
    if not p or not p.get("extra_label"):
        await update.message.reply_text(T("session_expired"))
        return ConversationHandler.END
    t = update.message.text.strip()
    if p.get("extra_type") == "number":
        t = t.translate(AR_DIGITS).replace(" ", "")
        if not re.fullmatch(r"\d{1,30}", t):
            await update.message.reply_text(T("extra_bad_num"))
            return EXTRA
    elif not (1 <= len(t) <= 300):
        await update.message.reply_text(T("extra_bad_text"))
        return EXTRA
    d["extra"] = t
    await send_summary(update.message, context)
    return CONFIRM


async def buy_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pid = int(q.data.split("_")[1])
    state = availability(pid, q.from_user.id)
    if state != "available":
        await q.message.reply_text(T("unavail_" + state))
        return ConversationHandler.END
    p = get_product(pid)
    context.user_data.clear()
    context.user_data["pid"] = pid

    if not p["shipping"]:  # منتج رقمي: لا هاتف ولا ولاية ولا عنوان
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
    # نخزّن الاسمين (عربي / English) ليقرأها الأدمن بأي لغة
    context.user_data["wilaya"] = f"{idx:02d} - {WILAYAS[idx - 1]} / {WILAYAS_EN[idx - 1]}"
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


async def pay_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = context.user_data
    pid = d.get("pid")
    p = get_product(pid) if pid else None
    if not p:
        await q.message.reply_text(T("session_expired"))
        return ConversationHandler.END
    if p["shipping"] and not all(k in d for k in ("phone", "wilaya", "address")):
        await q.message.reply_text(T("session_expired"))
        return ConversationHandler.END
    if p.get("extra_label") and not d.get("extra"):
        await q.message.reply_text(T("session_expired"))
        return ConversationHandler.END

    oid, why = place_order(q.from_user, pid, d.get("phone"), d.get("wilaya"), d.get("address"), d.get("extra"))
    if not oid:
        await q.message.reply_text(T("unavail_" + (why if why in ("sold", "held") else "sold")))
        return ConversationHandler.END
    order = get_order(oid)

    await q.message.reply_text(T("reserved", m=RESERVE_MINUTES))
    desc = (p["description"] or p["name"])
    if p["shipping"]:
        desc = T("invoice_ship_desc", name=p["name"])
    await context.bot.send_invoice(
        chat_id=q.message.chat_id,
        title=p["name"][:32],
        description=desc[:255],
        payload=f"order_{oid}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(p["name"][:32], order["amount"])],
    )

    # ── خيار الدفع بعملة TON (إن حدّد الأدمن سعراً بالتون ومحفظة) ──
    wallet = get_wallet()
    if wallet and p.get("price_ton"):
        nano = to_nano(p["price_ton"])
        with cursor() as cur:
            cur.execute("UPDATE shop_orders SET ton_nano=? WHERE id=?", (nano, oid))
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


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(T("cancelled"), reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text(T("cancelled"), reply_markup=ReplyKeyboardRemove())
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
        await q.answer(ok=False, error_message=T("precheck_fail"))


async def notify_admin(context, build):
    """build: دالة تأخذ لغة الأدمن وتُرجع النص (فتصل الرسالة بلغة الأدمن لا الزبون)."""
    a = shop_admin()
    if a:
        try:
            text = build(get_shop_lang(shop_id()))
            await context.bot.send_message(a, text)
        except Exception as e:
            log.error("admin notify failed: %s", e)


async def deliver_digital(context, chat_id, p):
    """يرسل المحتوى الرقمي للزبون. يرجع True إذا أُرسل شيء."""
    sent = False
    if p.get("delivery_text"):
        await context.bot.send_message(chat_id, T("your_product", text=esc(p["delivery_text"])))
        sent = True
    if p.get("delivery_file"):
        await context.bot.send_document(chat_id, p["delivery_file"])
        sent = True
    return sent


async def finalize_paid(context, uid, oid, status, order, charge, stars=True):
    """منطق ما بعد الدفع المشترك بين النجوم و TON."""
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

    # soldout / invalid
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
        # TON: لا استرجاع تلقائي، يلزم تدخل الأدمن
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


# ───────────────────────── الدفع بعملة TON ─────────────────────────
TON_TASKS = set()


def ton_memo(oid):
    return f"order_{oid}"


def to_nano(ton):
    return int(round(float(ton) * 1_000_000_000))


async def find_ton_payment(wallet, oid, min_nano, since):
    """يبحث في آخر معاملات المحفظة عن تحويل بنفس التعليق وبمبلغ كافٍ. يرجع الهاش أو None."""
    if not wallet or not min_nano:
        return None
    params = {"address": wallet, "limit": 50}
    if TONCENTER_KEY:
        params["api_key"] = TONCENTER_KEY
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get("https://toncenter.com/api/v2/getTransactions", params=params)
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
        if not m.get("source"):           # تجاهل المعاملات غير الواردة
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
    status, order = mark_paid(o["id"], charge)
    await finalize_paid(context, o["user_id"], o["id"], status, order, charge, stars=False)
    return status


async def watch_ton(context, oid):
    """يراقب المحفظة حتى 10 دقائق ثم يتوقف (يبقى زر «تحقق» متاحاً)."""
    try:
        for _ in range(60):
            await asyncio.sleep(10)
            o = get_order(oid)
            if not o or o["status"] != "pending":
                return
            h = await find_ton_payment(get_wallet(), oid, o.get("ton_nano"), o.get("created_at"))
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


async def cmd_setwallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text(T("wallet_usage", w=esc(get_wallet() or "—")))
        return
    w = context.args[0].strip()
    if not WALLET_RE.match(w):
        await update.message.reply_text(T("wallet_bad"))
        return
    set_wallet(w)
    await update.message.reply_text(T("wallet_ok"))


# ───────────────────────── لوحة الأدمن: المنتجات ─────────────────────────
def adm(context):
    return context.user_data.setdefault("adm", {})


def panel_text(p):
    n = len(get_photos(p["id"]))
    t = (
        f"🗂 <b>{esc(p['name'])}</b>  (#{p['id']})\n\n"
        f"{T('pt_type_ship') if p['shipping'] else T('pt_type_dig')}\n"
        f"{T('pt_price', p=p['price'])}\n"
    )
    if p.get("price_ton"):
        t += T("pt_ton", p=f"{fmt_ton(p['price_ton'])} TON") + "\n"
    t += (
        f"{T('pt_stock', s=stock_label(p['stock']))}\n"
        f"{T('pt_media', n=n)}\n"
        f"{T('pt_visible') if p['active'] else T('pt_hidden')}\n"
    )
    if p.get("extra_label"):
        kind = T("kind_num") if p.get("extra_type") == "number" else T("kind_text")
        t += T("pt_extra", label=esc(p["extra_label"]), kind=kind) + "\n"
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
        [B(T("b_to_dig") if p["shipping"] else T("b_to_phys"), callback_data=f"adm_ship_{i}")],
    ]
    if not p["shipping"]:
        rows.append([B(T("b_deliv"), callback_data=f"edt_deliv_{i}")])
    rows.append([B(T("b_extra"), callback_data=f"edt_extra_{i}")])
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
        f"{'✅' if p['active'] else '🚫'} {'🛠' if p['shipping'] else '💾'} {p['name'][:30]} — {p['price']}⭐",
        callback_data=f"adm_panel_{p['id']}")] for p in prods]
    rows.append([InlineKeyboardButton(T("b_add"), callback_data="adm_new")])
    await message.reply_text(T("products_title"), reply_markup=InlineKeyboardMarkup(rows))


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
        await q.message.reply_text(T("not_found"))
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
    await update.message.reply_text(T("ask_price"))
    return A_PRICE


async def got_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if not t.isdigit() or not (1 <= int(t) <= 1000000):
        await update.message.reply_text(T("bad_price"))
        return A_PRICE
    adm(context)["price"] = int(t)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(T("b_t_ship"), callback_data="nt_ship")],
        [InlineKeyboardButton(T("b_t_dig"), callback_data="nt_dig")],
    ])
    await update.message.reply_text(T("pick_type"), reply_markup=kb)
    return A_TYPE


async def got_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ship = 1 if q.data == "nt_ship" else 0
    adm(context)["shipping"] = ship
    if ship:
        await q.message.reply_text(T("ask_stock_ship"))
    else:
        await q.message.reply_text(T("ask_stock_dig"))
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


async def create_draft(update, context, desc):
    a = adm(context)
    a["pid"] = add_product(a["name"], desc, a["price"], a["stock"], a["shipping"], active=0)
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
    n = len(get_photos(pid))
    if n >= MAX_PHOTOS:
        await update.message.reply_text(T("media_max", n=MAX_PHOTOS))
        return A_PHOTOS
    if update.message.video:
        add_photo(pid, update.message.video.file_id, "video")
        what = T("what_video")
    else:
        add_photo(pid, update.message.photo[-1].file_id, "photo")
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


async def ask_delivery(message):
    await message.reply_text(T("ask_delivery"))


async def photos_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    p = get_product(a["pid"])
    if a.get("mode") == "new" and p and not p["shipping"]:
        await ask_delivery(update.message)
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


# ── تعديل منتج (دخول من الأزرار) ──
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
        await ask_delivery(q.message)
        return A_DELIV
    if field == "extra":
        await q.message.reply_text(T("x_ask"))
        return X_LABEL
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
        update_product(pid, price=int(t))
    elif field == "ton":
        if t == "-":
            update_product(pid, price_ton=None)
        else:
            try:
                v = float(t.translate(AR_DIGITS).replace(",", ".").replace(" ", ""))
            except ValueError:
                v = 0
            if not (0 < v <= 1000000):
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


async def got_x_label(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = adm(context)
    t = update.message.text.strip()
    if t == "-":
        pid = a["pid"]
        update_product(pid, extra_label=None, extra_type=None)
        context.user_data.pop("adm", None)
        await update.message.reply_text(T("x_removed"))
        await render_panel(update.message, pid)
        return ConversationHandler.END
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
    update_product(pid, extra_label=a["xlabel"], extra_type="number" if q.data == "xt_num" else "text")
    context.user_data.pop("adm", None)
    await q.message.reply_text(T("x_saved"))
    await render_panel(q.message, pid)
    return ConversationHandler.END


async def adm_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    a = context.user_data.pop("adm", {})
    if a.get("mode") == "new" and a.get("pid"):
        delete_product(a["pid"])  # مسودة غير مكتملة
    await update.message.reply_text(T("adm_cancelled"))
    return ConversationHandler.END


# ───────────────────────── أوامر الأدمن: الطلبات ─────────────────────────
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
            # رسالة الزبون بلغته هو
            await context.bot.send_message(
                o["user_id"], T("shipped_user", id=o["id"]))
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
        # دفع TON: الاسترجاع يدوي من محفظتك، لا يمكن عبر Telegram Stars
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
        await context.bot.send_message(
            o["user_id"], T("refunded_user", id=o["id"]))
    except Exception:
        pass


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    t = T("help_admin") + T("help_ton")
    if shop_id() == 0:
        t += T("help_multi")
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
        await context.bot.send_message(chat, T("tok_main"))
        return
    try:
        async with Bot(token) as b:
            me = await b.get_me()
    except Exception as e:
        await context.bot.send_message(chat, T("tok_invalid", err=esc(str(e)[:150])))
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


async def bind_shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يعمل أولاً مع كل تحديث: يحدد المتجر ولغة المستخدم."""
    shop = context.application.bot_data["shop"]
    _shop.set(shop)
    _lang.set(get_shop_lang(shop["id"]))


async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled error", exc_info=context.error)


def admin_commands(lang, is_main):
    cmds = [
        BotCommand("addproduct", T("cmd_addproduct", lang)),
        BotCommand("products", T("cmd_products", lang)),
        BotCommand("orders", T("cmd_orders", lang)),
        BotCommand("shipped", T("cmd_shipped", lang)),
        BotCommand("refund", T("cmd_refund", lang)),
        BotCommand("setwallet", T("cmd_setwallet", lang)),
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


async def set_admin_commands(bot, admin_id, sid):
    if not admin_id:
        return
    try:
        lang = get_shop_lang(sid)
        await bot.set_my_commands(admin_commands(lang, sid == 0), scope=BotCommandScopeChat(admin_id))
    except Exception as e:
        log.warning("admin set_my_commands failed: %s", e)


def user_commands(lang):
    return [BotCommand("start", T("cmd_start", lang))]


async def setup_commands(bot, admin_id, sid):
    try:
        lang = get_shop_lang(sid)
        cmds = user_commands(lang)
        await bot.set_my_commands(cmds, scope=BotCommandScopeDefault())
        for lg in LANGS:  # نفس اللغة للجميع مهما كانت لغة هاتفهم
            await bot.set_my_commands(cmds, scope=BotCommandScopeDefault(), language_code=lg)
    except Exception as e:
        log.warning("default set_my_commands failed: %s", e)
    await set_admin_commands(bot, admin_id, sid)


# ───────────────────────── تشغيل البوتات الفرعية ─────────────────────────
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


async def post_init(app: Application):
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
    msg += T("su_help", L)
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

    # يحدد المتجر واللغة قبل أي handler آخر (group -1)
    app.add_handler(TypeHandler(Update, bind_shop), group=-1)

    admin_conv = ConversationHandler(
        entry_points=[
            CommandHandler("addproduct", cmd_addproduct),
            CallbackQueryHandler(new_product_cb, pattern=r"^adm_new$"),
            CallbackQueryHandler(edit_entry, pattern=r"^edt_(name|desc|price|ton|stock|photos|deliv|extra)_\d+$"),
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
            X_LABEL: [MessageHandler(text_only, got_x_label)],
            X_TYPE: [CallbackQueryHandler(got_x_type, pattern=r"^xt_(num|text)$")],
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
            EXTRA: [MessageHandler(text_only, got_extra)],
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
    app.add_handler(CommandHandler("lang", cmd_lang))
    app.add_handler(CallbackQueryHandler(lang_cb, pattern=r"^setlang_(ar|en)$"))
    app.add_handler(CallbackQueryHandler(prod_cb, pattern=r"^prod_\d+$"))
    app.add_handler(CallbackQueryHandler(list_cb, pattern=r"^list$"))
    app.add_handler(PreCheckoutQueryHandler(pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, on_paid))
    app.add_handler(CallbackQueryHandler(ton_check_cb, pattern=r"^tonchk_\d+$"))

    app.add_handler(CallbackQueryHandler(adm_list_cb, pattern=r"^adm_list$"))
    app.add_handler(CallbackQueryHandler(adm_cb, pattern=r"^adm_(panel|ship|active|clrphotos|del|delyes)_\d+$"))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("products", cmd_products))
    app.add_handler(CommandHandler("orders", cmd_orders))
    app.add_handler(CommandHandler("shipped", cmd_shipped))
    app.add_handler(CommandHandler("refund", cmd_refund))
    app.add_handler(CommandHandler("setwallet", cmd_setwallet))

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
