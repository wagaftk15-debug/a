import os
import json
import requests
import psycopg2
from psycopg2 import pool
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from threading import Lock
import time
import logging
from collections import defaultdict

# ───────────────────────── إعدادات اللوجينج ─────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("StarTonBot")

app = Flask(__name__)

# ───────────────────────── الإعدادات ─────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")

# سعر الصرف: 1000 نجمة = 1 TON
STARS_PER_TON = 1000
MIN_EXCHANGE_STARS = 100          # أقل عدد نجوم مسموح بتبديله
MAX_EXCHANGE_STARS = 1_000_000    # أعلى عدد نجوم مسموح بتبديله في الطلب الواحد

EXCHANGE_AMOUNTS = [1000, 2000, 5000, 10000, 20000]

REQUEST_TIMEOUT = 10

# ───────────────────────── Connection Pool ─────────────────────────
db_pool = None
pool_lock = Lock()
MAX_RETRIES = 3
RETRY_DELAY = 0.4

# ───────────────────────── Cache + Rate Limit ─────────────────────────
cache = {}
cache_lock = Lock()
CACHE_TTL = {
    'stats': 30,
    'myrequests': 15,
}

rate_limit = defaultdict(list)
rate_lock = Lock()
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 25

# حالة المستخدم أثناء تعبئة طلب التبديل (بالذاكرة)
# user_states[user_id] = "waiting_wallet" | "waiting_stars_custom"
user_states = {}
state_lock = Lock()

# تخزين مؤقت لعنوان المحفظة بين خطوة إدخال العنوان وخطوة اختيار المبلغ
pending_wallet = {}
wallet_lock = Lock()


def check_rate_limit(key: str) -> bool:
    """يرجع True إذا مسموح بالطلب"""
    now = time.time()
    with rate_lock:
        rate_limit[key] = [t for t in rate_limit[key] if now - t < RATE_LIMIT_WINDOW]
        if len(rate_limit[key]) >= RATE_LIMIT_MAX:
            return False
        rate_limit[key].append(now)
        return True


# ───────────────────────── دوال مساعدة ─────────────────────────
def stars_to_ton(stars: int) -> float:
    return round(stars / STARS_PER_TON, 4)


def format_ton(amount: float) -> str:
    return f"{amount:.4f}".rstrip('0').rstrip('.') if '.' in f"{amount:.4f}" else f"{amount:.4f}"


def is_valid_wallet(address: str) -> bool:
    """تحقق بسيط من شكل عنوان محفظة TON (لا يضمن صحتها الفعلية)"""
    if not address:
        return False
    address = address.strip()
    if len(address) < 30 or len(address) > 68:
        return False
    return address.startswith(("UQ", "EQ", "kQ", "0Q")) or address.isalnum()


def mask_name(name: str) -> str:
    if not name:
        return "****"
    name = str(name).strip()[:18]
    if len(name) <= 2:
        return name[0] + "*"
    return name[0] + "*" * (len(name) - 2) + name[-1]


def status_label(status: str) -> str:
    return {
        "pending": "⏳ بانتظار المراجعة",
        "accepted": "✅ تم القبول - بانتظار الدفع",
        "rejected": "❌ مرفوض",
        "paid": "💰 تم الدفع - بانتظار تحويل TON",
        "completed": "🎉 مكتمل",
    }.get(status, status)


# ───────────────────────── Database ─────────────────────────
def init_pool():
    global db_pool
    if not DATABASE_URL:
        logger.error("DATABASE_URL غير موجود")
        return False
    try:
        with pool_lock:
            if db_pool is not None:
                return True
            db_pool = pool.SimpleConnectionPool(
                2, 25, DATABASE_URL,
                connect_timeout=8,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
        logger.info("✅ Connection Pool جاهز")
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في Pool: {e}")
        return False


def get_connection(retry=0):
    global db_pool
    if db_pool is None:
        if not init_pool():
            raise Exception("فشل إنشاء Pool")
    try:
        return db_pool.getconn()
    except Exception as e:
        if retry < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return get_connection(retry + 1)
        raise Exception(f"فشل الاتصال: {str(e)[:80]}")


def return_connection(conn, close=False):
    global db_pool
    if conn is None:
        return
    try:
        if close or db_pool is None:
            conn.close()
        else:
            db_pool.putconn(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def get_cache(key):
    with cache_lock:
        if key in cache:
            data, timestamp = cache[key]
            ttl = CACHE_TTL.get(key.split('_')[0], 30)
            if data is not None and datetime.now() - timestamp < timedelta(seconds=ttl):
                return data
            try:
                del cache[key]
            except Exception:
                pass
    return None


def set_cache(key, data):
    if data is None:
        return
    with cache_lock:
        cache[key] = (data, datetime.now())


def clear_cache(pattern=None):
    with cache_lock:
        if pattern is None:
            cache.clear()
        else:
            keys = [k for k in list(cache.keys()) if pattern in k]
            for k in keys:
                try:
                    del cache[k]
                except Exception:
                    pass


def init_db():
    if not DATABASE_URL:
        return
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS exchange_requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                stars_amount INTEGER NOT NULL,
                ton_amount NUMERIC(18, 4) NOT NULL,
                ton_wallet TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                telegram_payment_charge_id TEXT,
                admin_note TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                processed_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_exchange_user ON exchange_requests(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_exchange_status ON exchange_requests(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_exchange_created ON exchange_requests(created_at DESC)")

        conn.commit()
        cur.close()
        logger.info("✅ قاعدة البيانات جاهزة")
    except Exception as e:
        logger.error(f"❌ خطأ في DB: {e}")
    finally:
        return_connection(conn)


# ───────────────────────── Users ─────────────────────────
def upsert_user(user_id, username=None, first_name=None):
    if not user_id:
        return
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (user_id, username, first_name, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET username = COALESCE(EXCLUDED.username, users.username),
                first_name = COALESCE(EXCLUDED.first_name, users.first_name),
                updated_at = NOW()
        """, (user_id, username or '', first_name or ''))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error(f"upsert_user error: {e}")
    finally:
        return_connection(conn)


def get_display_name(user_id):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT username, first_name FROM users WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
        if row:
            return row[0] or row[1] or str(user_id)
        return str(user_id)
    except Exception as e:
        logger.error(f"get_display_name error: {e}")
        return str(user_id)
    finally:
        return_connection(conn)


# ───────────────────────── Exchange Requests ─────────────────────────
def create_exchange_request(user_id, stars_amount, ton_wallet):
    conn = None
    try:
        ton_amount = stars_to_ton(stars_amount)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO exchange_requests (user_id, stars_amount, ton_amount, ton_wallet, status)
            VALUES (%s, %s, %s, %s, 'pending')
            RETURNING id
        """, (user_id, stars_amount, ton_amount, ton_wallet))
        request_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        clear_cache('myrequests')
        clear_cache('stats')
        return request_id
    except Exception as e:
        logger.error(f"create_exchange_request error: {e}")
        return None
    finally:
        return_connection(conn)


def get_exchange_request(request_id):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, user_id, stars_amount, ton_amount, ton_wallet, status,
                   telegram_payment_charge_id, admin_note
            FROM exchange_requests WHERE id = %s
        """, (request_id,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return {
            "id": row[0], "user_id": row[1], "stars_amount": row[2],
            "ton_amount": float(row[3]), "ton_wallet": row[4], "status": row[5],
            "charge_id": row[6], "admin_note": row[7],
        }
    except Exception as e:
        logger.error(f"get_exchange_request error: {e}")
        return None
    finally:
        return_connection(conn)


def update_exchange_status(request_id, status, charge_id=None, admin_note=None, touch_completed=False):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        sets = ["status = %s", "processed_at = NOW()"]
        params = [status]
        if charge_id is not None:
            sets.append("telegram_payment_charge_id = %s")
            params.append(charge_id)
        if admin_note is not None:
            sets.append("admin_note = %s")
            params.append(admin_note[:500])
        if touch_completed:
            sets.append("completed_at = NOW()")
        params.append(request_id)
        cur.execute(f"UPDATE exchange_requests SET {', '.join(sets)} WHERE id = %s", params)
        updated = cur.rowcount > 0
        conn.commit()
        cur.close()
        if updated:
            clear_cache('myrequests')
            clear_cache('stats')
        return updated
    except Exception as e:
        logger.error(f"update_exchange_status error: {e}")
        return False
    finally:
        return_connection(conn)


def get_user_requests(user_id, limit=5):
    key = f"myrequests_{user_id}"
    cached = get_cache(key)
    if cached is not None:
        return cached
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, stars_amount, ton_amount, status, created_at
            FROM exchange_requests
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (user_id, limit))
        rows = cur.fetchall()
        cur.close()
        result = [
            {"id": r[0], "stars_amount": r[1], "ton_amount": float(r[2]),
             "status": r[3], "created_at": r[4]}
            for r in rows
        ]
        set_cache(key, result)
        return result
    except Exception as e:
        logger.error(f"get_user_requests error: {e}")
        return []
    finally:
        return_connection(conn)


def get_admin_stats():
    cached = get_cache('stats')
    if cached is not None:
        return cached
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT status, COUNT(*), COALESCE(SUM(stars_amount), 0), COALESCE(SUM(ton_amount), 0)
            FROM exchange_requests GROUP BY status
        """)
        rows = cur.fetchall()
        cur.close()
        result = {r[0]: {"count": r[1], "stars": r[2], "ton": float(r[3])} for r in rows}
        set_cache('stats', result)
        return result
    except Exception as e:
        logger.error(f"get_admin_stats error: {e}")
        return {}
    finally:
        return_connection(conn)


# ───────────────────────── Telegram API ─────────────────────────
def send_message(chat_id, text, reply_markup=None):
    if not chat_id or not text:
        return False
    try:
        payload = {
            "chat_id": int(chat_id),
            "text": text[:4096],
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUEST_TIMEOUT)
        return r.json().get("ok", False)
    except Exception as e:
        logger.error(f"send_message error: {e}")
        return False


def answer_callback(callback_id, text="", show_alert=False):
    try:
        requests.post(
            f"{TELEGRAM_API}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text[:200], "show_alert": show_alert},
            timeout=REQUEST_TIMEOUT
        )
    except Exception as e:
        logger.error(f"answer_callback error: {e}")


def send_invoice(chat_id, amount, title, description, payload_str):
    if not chat_id or amount <= 0:
        return False
    try:
        payload = {
            "chat_id": int(chat_id),
            "title": title[:32],
            "description": description[:255],
            "payload": payload_str[:128],
            "provider_token": "",
            "currency": "XTR",
            "prices": [{"label": title[:32], "amount": int(amount)}],
            "start_parameter": payload_str[:64],
        }
        r = requests.post(f"{TELEGRAM_API}/sendInvoice", json=payload, timeout=REQUEST_TIMEOUT)
        result = r.json()
        if not result.get('ok'):
            send_message(chat_id, "⚠️ حصل خطأ في إنشاء فاتورة الدفع، جرب مرة ثانية.")
            return False
        return True
    except Exception as e:
        logger.error(f"send_invoice error: {e}")
        return False


def answer_pre_checkout(pre_checkout_query_id, ok=True, error_message=None):
    try:
        payload = {"pre_checkout_query_id": pre_checkout_query_id, "ok": ok}
        if error_message:
            payload["error_message"] = error_message[:200]
        requests.post(f"{TELEGRAM_API}/answerPreCheckoutQuery", json=payload, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        logger.error(f"answer_pre_checkout error: {e}")


def notify_admin(text, reply_markup=None):
    if ADMIN_CHAT_ID:
        send_message(ADMIN_CHAT_ID, text, reply_markup)


# ───────────────────────── Keyboards ─────────────────────────
def main_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "🔄 تبديل نجوم بـ TON", "callback_data": "exchange_start"}],
            [{"text": "📋 طلباتي", "callback_data": "my_requests"}],
            [{"text": "❓ المساعدة", "callback_data": "help"}],
        ]
    }


def exchange_amount_keyboard():
    keyboard = []
    for amount in EXCHANGE_AMOUNTS:
        ton = format_ton(stars_to_ton(amount))
        keyboard.append([{"text": f"⭐ {amount:,} ← {ton} TON", "callback_data": f"exchange_amt_{amount}"}])
    keyboard.append([{"text": "📝 مبلغ مخصص", "callback_data": "exchange_custom"}])
    keyboard.append([{"text": "🔙 رجوع", "callback_data": "back_main"}])
    return {"inline_keyboard": keyboard}


def admin_decision_keyboard(request_id):
    return {
        "inline_keyboard": [
            [
                {"text": "✅ قبول", "callback_data": f"exchange_accept_{request_id}"},
                {"text": "❌ رفض", "callback_data": f"exchange_reject_{request_id}"},
            ]
        ]
    }


def admin_mark_done_keyboard(request_id):
    return {
        "inline_keyboard": [
            [{"text": "✅ تم تحويل الـ TON", "callback_data": f"exchange_done_{request_id}"}]
        ]
    }


def build_my_requests_text(user_id):
    reqs = get_user_requests(user_id, 5)
    if not reqs:
        return "لا توجد طلبات تبديل سابقة 🙂"
    lines = ["📋 <b>آخر طلباتك:</b>\n"]
    for r in reqs:
        lines.append(
            f"#{r['id']} • ⭐ {r['stars_amount']:,} ← {format_ton(r['ton_amount'])} TON\n"
            f"الحالة: {status_label(r['status'])}"
        )
    return "\n\n".join(lines)


# ───────────────────────── State Helpers ─────────────────────────
def set_state(user_id, state):
    with state_lock:
        if state is None:
            user_states.pop(user_id, None)
        else:
            user_states[user_id] = state


def get_state(user_id):
    with state_lock:
        return user_states.get(user_id)


def set_pending_wallet(user_id, wallet):
    with wallet_lock:
        pending_wallet[user_id] = wallet


def pop_pending_wallet(user_id):
    with wallet_lock:
        return pending_wallet.pop(user_id, None)


# ───────────────────────── Exchange Flow ─────────────────────────
def start_exchange_request(user_id, chat_id, stars_amount, wallet, callback_id=None):
    if stars_amount < MIN_EXCHANGE_STARS:
        msg = f"❌ أقل عدد نجوم للتبديل هو {MIN_EXCHANGE_STARS:,} ⭐"
        if callback_id:
            answer_callback(callback_id, msg, show_alert=True)
        else:
            send_message(chat_id, msg)
        return
    if stars_amount > MAX_EXCHANGE_STARS:
        msg = f"❌ أقصى عدد نجوم للتبديل هو {MAX_EXCHANGE_STARS:,} ⭐ لكل طلب"
        if callback_id:
            answer_callback(callback_id, msg, show_alert=True)
        else:
            send_message(chat_id, msg)
        return

    request_id = create_exchange_request(user_id, stars_amount, wallet)
    if not request_id:
        msg = "⚠️ حصل خطأ أثناء إنشاء الطلب، حاول مرة ثانية."
        if callback_id:
            answer_callback(callback_id, msg, show_alert=True)
        else:
            send_message(chat_id, msg)
        return

    ton_amount = stars_to_ton(stars_amount)
    if callback_id:
        answer_callback(callback_id, "✅ تم إرسال طلبك")

    send_message(
        chat_id,
        f"✅ تم استلام طلب التبديل رقم <b>#{request_id}</b>\n\n"
        f"⭐ النجوم: <b>{stars_amount:,}</b>\n"
        f"💎 مقابل: <b>{format_ton(ton_amount)} TON</b>\n"
        f"👛 المحفظة: <code>{wallet}</code>\n\n"
        f"⏳ طلبك الآن قيد المراجعة من الإدارة، بنعلمك فور اتخاذ القرار."
    )

    display_name = get_display_name(user_id)
    notify_admin(
        f"🆕 <b>طلب تبديل جديد</b> #{request_id}\n\n"
        f"👤 المستخدم: {display_name} (<code>{user_id}</code>)\n"
        f"⭐ النجوم: <b>{stars_amount:,}</b>\n"
        f"💎 يعادل: <b>{format_ton(ton_amount)} TON</b>\n"
        f"👛 المحفظة: <code>{wallet}</code>\n\n"
        f"اقبل أو ارفض الطلب:",
        admin_decision_keyboard(request_id)
    )


# ───────────────────────── Webhook ─────────────────────────
@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    if not check_rate_limit("webhook"):
        return jsonify({"ok": True}), 429

    update = request.get_json(force=True, silent=True) or {}

    # Pre-checkout: نوافق مباشرة دائماً (التحقق الفعلي تم قبل إرسال الفاتورة)
    if "pre_checkout_query" in update:
        pcq = update["pre_checkout_query"]
        answer_pre_checkout(pcq["id"], ok=True)
        return jsonify({"ok": True})

    # ───────────── Message ─────────────
    if "message" in update:
        msg = update["message"]
        chat_id = msg.get("chat", {}).get("id")
        text = (msg.get("text") or "").strip()
        sender = msg.get("from", {})
        user_id = sender.get("id")

        if user_id:
            upsert_user(user_id, username=sender.get("username"), first_name=sender.get("first_name"))

        # دفعة ناجحة (تحويل النجوم فعلياً)
        if "successful_payment" in msg:
            sp = msg["successful_payment"]
            amount = sp.get("total_amount", 0)
            charge_id = sp.get("telegram_payment_charge_id", "")
            invoice_payload = sp.get("invoice_payload", "")

            if invoice_payload.startswith("exchange_") and user_id:
                try:
                    request_id = int(invoice_payload.split("_")[1])
                except Exception:
                    request_id = None

                req = get_exchange_request(request_id) if request_id else None
                if req and req["status"] == "accepted":
                    update_exchange_status(request_id, "paid", charge_id=charge_id)
                    send_message(
                        chat_id,
                        f"💛 تم استلام <b>{amount:,}⭐</b> بنجاح لطلبك #{request_id}\n\n"
                        f"سيتم تحويل <b>{format_ton(req['ton_amount'])} TON</b> إلى محفظتك خلال وقت قصير 🙏"
                    )
                    display_name = get_display_name(user_id)
                    notify_admin(
                        f"💰 <b>دفعة مستلمة</b> - طلب #{request_id}\n\n"
                        f"👤 {display_name} (<code>{user_id}</code>)\n"
                        f"⭐ {amount:,} تم استلامها\n"
                        f"👛 حوّل <b>{format_ton(req['ton_amount'])} TON</b> إلى:\n<code>{req['ton_wallet']}</code>",
                        admin_mark_done_keyboard(request_id)
                    )
                else:
                    logger.error(f"successful_payment: exchange request {request_id} not in expected state")
                    send_message(chat_id, "⚠️ تم استلام الدفعة، لكن حصل خطأ بتحديث الطلب. تواصل مع الدعم رجاءً.")
            return jsonify({"ok": True})

        # ───── حالة انتظار إدخال عنوان المحفظة ─────
        state = get_state(user_id)

        if state == "waiting_wallet" and text and not text.startswith("/"):
            if not is_valid_wallet(text):
                send_message(chat_id, "❌ عنوان المحفظة غير صحيح. تأكدي من نسخه بشكل صحيح من محفظتك (Tonkeeper, Tonhub...الخ) وأرسليه مرة ثانية.")
                return jsonify({"ok": True})
            set_pending_wallet(user_id, text)
            set_state(user_id, None)
            send_message(chat_id, "👛 تم استلام عنوان المحفظة ✅\n\nالآن اختاري عدد النجوم التي تريدين تبديلها:", exchange_amount_keyboard())
            return jsonify({"ok": True})

        # ───── حالة انتظار إدخال مبلغ نجوم مخصص ─────
        if state == "waiting_stars_custom" and text and not text.startswith("/"):
            wallet = pop_pending_wallet(user_id)
            if not wallet:
                set_state(user_id, None)
                send_message(chat_id, "⚠️ انتهت صلاحية الجلسة، ابدئي من جديد بالضغط على 🔄 تبديل نجوم بـ TON")
                return jsonify({"ok": True})
            digits = ''.join(filter(str.isdigit, text))
            if not digits:
                send_message(chat_id, "❌ يجب إدخال رقم صحيح، مثال: 3000")
                set_pending_wallet(user_id, wallet)
                return jsonify({"ok": True})
            stars_amount = int(digits)
            set_state(user_id, None)
            start_exchange_request(user_id, chat_id, stars_amount, wallet)
            return jsonify({"ok": True})

        # ───── الأوامر ─────
        if text.startswith("/start"):
            send_message(
                chat_id,
                "👋 أهلاً بك في <b>بوت تبديل النجوم بعملة TON</b> 💎\n\n"
                f"💱 سعر الصرف الحالي: <b>{STARS_PER_TON:,} ⭐ = 1 TON</b>\n\n"
                "طريقة العمل:\n"
                "1️⃣ تقدّمي بطلب تبديل (نرسل لك عنوان محفظتك)\n"
                "2️⃣ الإدارة تراجع الطلب وتقبله أو ترفضه\n"
                "3️⃣ إذا تم القبول، تدفعي النجوم عبر تيليجرام\n"
                "4️⃣ نحوّل لك مبلغ TON المقابل إلى محفظتك\n\n"
                "اختاري من القائمة:",
                main_keyboard()
            )

        elif text in ("/طلباتي", "/requests", "/myrequests"):
            send_message(chat_id, build_my_requests_text(user_id))

        elif text in ("/مساعدة", "/help"):
            send_message(
                chat_id,
                "❓ <b>عن البوت</b>\n\n"
                f"يبدّل البوت نجوم تيليجرام مقابل عملة TON بسعر ثابت:\n"
                f"<b>{STARS_PER_TON:,} ⭐ = 1 TON</b>\n\n"
                "كل طلب يمر بمراجعة يدوية من الإدارة قبل الدفع، وبعد الدفع "
                "يتم تحويل الـ TON يدوياً إلى محفظتك.\n\n"
                "الأوامر:\n"
                "/start - القائمة الرئيسية\n"
                "/طلباتي - عرض طلباتك السابقة"
            )

        # أوامر الأدمن
        elif text.startswith("/admin") and str(user_id) == str(ADMIN_CHAT_ID):
            stats = get_admin_stats()
            lines = ["🛠 <b>لوحة الأدمن</b>\n"]
            for status_key in ("pending", "accepted", "paid", "completed", "rejected"):
                s = stats.get(status_key, {"count": 0, "stars": 0, "ton": 0})
                lines.append(f"{status_label(status_key)}: {s['count']} طلب / {s['stars']:,}⭐ / {format_ton(s['ton'])} TON")
            send_message(chat_id, "\n".join(lines))

        elif text == "/clearcache" and str(user_id) == str(ADMIN_CHAT_ID):
            clear_cache()
            send_message(chat_id, "✅ تم مسح الـ Cache")

    # ───────────── Callback Query ─────────────
    elif "callback_query" in update:
        cq = update["callback_query"]
        sender = cq.get("from", {})
        user_id = sender.get("id")
        chat_id = cq.get("message", {}).get("chat", {}).get("id")
        callback_id = cq.get("id")
        data_key = cq.get("data", "")

        upsert_user(user_id, username=sender.get("username"), first_name=sender.get("first_name"))

        if data_key == "exchange_start":
            answer_callback(callback_id, "")
            set_state(user_id, "waiting_wallet")
            send_message(
                chat_id,
                "👛 أرسلي عنوان محفظة TON التي تريدين استلام العملة عليها\n\n"
                "مثال: <code>UQAbCdEf...</code>\n\n"
                "⚠️ تأكدي من صحة العنوان جيداً، البوت لا يتحمل مسؤولية تحويلات لعناوين خاطئة."
            )

        elif data_key == "my_requests":
            answer_callback(callback_id, "")
            send_message(chat_id, build_my_requests_text(user_id))

        elif data_key == "help":
            answer_callback(callback_id, "")
            send_message(
                chat_id,
                "❓ <b>عن البوت</b>\n\n"
                f"يبدّل البوت نجوم تيليجرام مقابل TON بسعر ثابت: <b>{STARS_PER_TON:,} ⭐ = 1 TON</b>\n\n"
                "كل طلب يمر بمراجعة يدوية قبل الدفع، وبعد الدفع تُحوَّل الـ TON يدوياً إلى محفظتك."
            )

        elif data_key == "back_main":
            answer_callback(callback_id, "")
            send_message(chat_id, "القائمة الرئيسية:", main_keyboard())

        elif data_key == "exchange_custom":
            wallet = None
            with wallet_lock:
                wallet = pending_wallet.get(user_id)
            if not wallet:
                answer_callback(callback_id, "أدخلي عنوان المحفظة أولاً", show_alert=True)
                set_state(user_id, "waiting_wallet")
                send_message(chat_id, "👛 أرسلي عنوان محفظة TON أولاً:")
                return jsonify({"ok": True})
            answer_callback(callback_id, "")
            set_state(user_id, "waiting_stars_custom")
            send_message(chat_id, f"📝 اكتبي عدد النجوم التي تريدين تبديلها (بالأرقام فقط)\nالحد الأدنى: {MIN_EXCHANGE_STARS:,} ⭐")

        elif data_key.startswith("exchange_amt_"):
            wallet = pop_pending_wallet(user_id)
            if not wallet:
                answer_callback(callback_id, "أدخلي عنوان المحفظة أولاً", show_alert=True)
                set_state(user_id, "waiting_wallet")
                send_message(chat_id, "👛 أرسلي عنوان محفظة TON أولاً:")
                return jsonify({"ok": True})
            try:
                amount = int(data_key.split("_")[2])
            except Exception:
                answer_callback(callback_id, "خطأ", show_alert=True)
                return jsonify({"ok": True})
            start_exchange_request(user_id, chat_id, amount, wallet, callback_id=callback_id)

        # ───── قرارات الأدمن ─────
        elif data_key.startswith("exchange_accept_") and str(user_id) == str(ADMIN_CHAT_ID):
            try:
                request_id = int(data_key.split("_")[2])
            except Exception:
                answer_callback(callback_id, "خطأ", show_alert=True)
                return jsonify({"ok": True})
            req = get_exchange_request(request_id)
            if not req or req["status"] != "pending":
                answer_callback(callback_id, "الطلب غير متاح للقبول", show_alert=True)
                return jsonify({"ok": True})
            update_exchange_status(request_id, "accepted")
            sent = send_invoice(
                req["user_id"], req["stars_amount"],
                f"تبديل {req['stars_amount']}⭐",
                f"مقابل {format_ton(req['ton_amount'])} TON",
                f"exchange_{request_id}"
            )
            if sent:
                answer_callback(callback_id, "✅ تم القبول وإرسال فاتورة الدفع للمستخدم")
                send_message(req["user_id"], f"✅ تم قبول طلبك #{request_id}!\nسيصلك الآن طلب دفع {req['stars_amount']:,}⭐، أكملي الدفع لإتمام التبديل.")
            else:
                answer_callback(callback_id, "تم القبول لكن فشل إرسال فاتورة الدفع", show_alert=True)

        elif data_key.startswith("exchange_reject_") and str(user_id) == str(ADMIN_CHAT_ID):
            try:
                request_id = int(data_key.split("_")[2])
            except Exception:
                answer_callback(callback_id, "خطأ", show_alert=True)
                return jsonify({"ok": True})
            req = get_exchange_request(request_id)
            if not req or req["status"] != "pending":
                answer_callback(callback_id, "الطلب غير متاح للرفض", show_alert=True)
                return jsonify({"ok": True})
            update_exchange_status(request_id, "rejected")
            answer_callback(callback_id, "❌ تم رفض الطلب")
            send_message(req["user_id"], f"❌ نعتذر، تم رفض طلب التبديل #{request_id}.\nيمكنك تقديم طلب جديد إذا رغبتِ.")

        elif data_key.startswith("exchange_done_") and str(user_id) == str(ADMIN_CHAT_ID):
            try:
                request_id = int(data_key.split("_")[2])
            except Exception:
                answer_callback(callback_id, "خطأ", show_alert=True)
                return jsonify({"ok": True})
            req = get_exchange_request(request_id)
            if not req or req["status"] != "paid":
                answer_callback(callback_id, "الطلب غير متاح للإكمال", show_alert=True)
                return jsonify({"ok": True})
            update_exchange_status(request_id, "completed", touch_completed=True)
            answer_callback(callback_id, "✅ تم تعليم الطلب كمكتمل")
            send_message(req["user_id"], f"🎉 تم تحويل <b>{format_ton(req['ton_amount'])} TON</b> إلى محفظتك بنجاح!\nشكراً لاستخدامك البوت 💎")

    return jsonify({"ok": True})


# ───────────────────────── Health Check ─────────────────────────
@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": datetime.now().isoformat(),
        "rate": f"{STARS_PER_TON} stars = 1 TON"
    })


# ───────────────────────── تشغيل ─────────────────────────
if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"🚀 البوت شغال على المنفذ {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
