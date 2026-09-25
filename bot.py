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

# ───────────────────────── Logging setup ─────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("StarTonBot")

app = Flask(__name__)

# ───────────────────────── Config ─────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")

# Public base URL of this deployment, e.g. https://your-app.up.railway.app
# On Railway, set this to the value shown under Settings -> Networking -> Public Domain
# (or leave PUBLIC_URL unset and instead set RAILWAY_PUBLIC_DOMAIN, which Railway
# injects automatically when a public domain is generated).
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")
if not PUBLIC_URL and os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
    PUBLIC_URL = f"https://{os.environ['RAILWAY_PUBLIC_DOMAIN']}"

# Optional shared secret Telegram will echo back on every webhook call, so we can
# reject requests that don't come from Telegram. Set the same value in both
# WEBHOOK_SECRET and when calling setWebhook (done automatically below).
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

# Exchange rate: 1000 Stars = 1 TON
STARS_PER_TON = 1000
MIN_EXCHANGE_STARS = 100          # minimum stars allowed per request
MAX_EXCHANGE_STARS = 1_000_000    # maximum stars allowed per single request

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

# In-memory state while a user is filling out an exchange request
# user_states[user_id] = "waiting_wallet" | "waiting_stars_custom"
user_states = {}
state_lock = Lock()

# Temporary storage for a wallet address between the "enter wallet" step
# and the "choose amount" step
pending_wallet = {}
wallet_lock = Lock()


def check_rate_limit(key: str) -> bool:
    """Returns True if the request is allowed"""
    now = time.time()
    with rate_lock:
        rate_limit[key] = [t for t in rate_limit[key] if now - t < RATE_LIMIT_WINDOW]
        if len(rate_limit[key]) >= RATE_LIMIT_MAX:
            return False
        rate_limit[key].append(now)
        return True


# ───────────────────────── Helpers ─────────────────────────
def stars_to_ton(stars: int) -> float:
    return round(stars / STARS_PER_TON, 4)


def format_ton(amount: float) -> str:
    return f"{amount:.4f}".rstrip('0').rstrip('.') if '.' in f"{amount:.4f}" else f"{amount:.4f}"


def is_valid_wallet(address: str) -> bool:
    """Basic sanity check on the shape of a TON wallet address (does not guarantee validity)"""
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
        "pending": "⏳ Awaiting review",
        "accepted": "✅ Accepted - awaiting payment",
        "rejected": "❌ Rejected",
        "paid": "💰 Paid - awaiting TON transfer",
        "completed": "🎉 Completed",
    }.get(status, status)


# ───────────────────────── Database ─────────────────────────
def init_pool():
    global db_pool
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set")
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
        logger.info("✅ Connection pool ready")
        return True
    except Exception as e:
        logger.error(f"❌ Pool error: {e}")
        return False


def get_connection(retry=0):
    global db_pool
    if db_pool is None:
        if not init_pool():
            raise Exception("Failed to create pool")
    try:
        return db_pool.getconn()
    except Exception as e:
        if retry < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return get_connection(retry + 1)
        raise Exception(f"Connection failed: {str(e)[:80]}")


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
        logger.info("✅ Database ready")
    except Exception as e:
        logger.error(f"❌ DB error: {e}")
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
            send_message(chat_id, "⚠️ Something went wrong creating the payment invoice, please try again.")
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


def set_webhook():
    """Registers our webhook URL with Telegram so updates actually get delivered here.
    Without this call, the server can run perfectly fine and still never receive anything."""
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN is not set - cannot register webhook")
        return False
    if not PUBLIC_URL:
        logger.warning(
            "⚠️ PUBLIC_URL / RAILWAY_PUBLIC_DOMAIN is not set - skipping automatic "
            "setWebhook call. The bot will NOT receive updates until the webhook is "
            "registered manually (see /set_webhook_info or Telegram's setWebhook API)."
        )
        return False
    webhook_url = f"{PUBLIC_URL}/webhook/{BOT_TOKEN}"
    try:
        payload = {"url": webhook_url, "allowed_updates": ["message", "callback_query", "pre_checkout_query"]}
        if WEBHOOK_SECRET:
            payload["secret_token"] = WEBHOOK_SECRET
        r = requests.post(f"{TELEGRAM_API}/setWebhook", json=payload, timeout=REQUEST_TIMEOUT)
        result = r.json()
        if result.get("ok"):
            logger.info(f"✅ Webhook registered: {webhook_url}")
            return True
        logger.error(f"❌ setWebhook failed: {result}")
        return False
    except Exception as e:
        logger.error(f"❌ setWebhook error: {e}")
        return False


# ───────────────────────── Keyboards ─────────────────────────
def main_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "🔄 Exchange Stars for TON", "callback_data": "exchange_start"}],
            [{"text": "📋 My Requests", "callback_data": "my_requests"}],
            [{"text": "❓ Help", "callback_data": "help"}],
        ]
    }


def exchange_amount_keyboard():
    keyboard = []
    for amount in EXCHANGE_AMOUNTS:
        ton = format_ton(stars_to_ton(amount))
        keyboard.append([{"text": f"⭐ {amount:,} → {ton} TON", "callback_data": f"exchange_amt_{amount}"}])
    keyboard.append([{"text": "📝 Custom amount", "callback_data": "exchange_custom"}])
    keyboard.append([{"text": "🔙 Back", "callback_data": "back_main"}])
    return {"inline_keyboard": keyboard}


def admin_decision_keyboard(request_id):
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Accept", "callback_data": f"exchange_accept_{request_id}"},
                {"text": "❌ Reject", "callback_data": f"exchange_reject_{request_id}"},
            ]
        ]
    }


def admin_mark_done_keyboard(request_id):
    return {
        "inline_keyboard": [
            [{"text": "✅ TON transferred", "callback_data": f"exchange_done_{request_id}"}]
        ]
    }


def build_my_requests_text(user_id):
    reqs = get_user_requests(user_id, 5)
    if not reqs:
        return "You have no previous exchange requests 🙂"
    lines = ["📋 <b>Your recent requests:</b>\n"]
    for r in reqs:
        lines.append(
            f"#{r['id']} • ⭐ {r['stars_amount']:,} → {format_ton(r['ton_amount'])} TON\n"
            f"Status: {status_label(r['status'])}"
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
        msg = f"❌ Minimum amount to exchange is {MIN_EXCHANGE_STARS:,} ⭐"
        if callback_id:
            answer_callback(callback_id, msg, show_alert=True)
        else:
            send_message(chat_id, msg)
        return
    if stars_amount > MAX_EXCHANGE_STARS:
        msg = f"❌ Maximum amount per request is {MAX_EXCHANGE_STARS:,} ⭐"
        if callback_id:
            answer_callback(callback_id, msg, show_alert=True)
        else:
            send_message(chat_id, msg)
        return

    request_id = create_exchange_request(user_id, stars_amount, wallet)
    if not request_id:
        msg = "⚠️ Something went wrong creating your request, please try again."
        if callback_id:
            answer_callback(callback_id, msg, show_alert=True)
        else:
            send_message(chat_id, msg)
        return

    ton_amount = stars_to_ton(stars_amount)
    if callback_id:
        answer_callback(callback_id, "✅ Your request has been submitted")

    send_message(
        chat_id,
        f"✅ Exchange request <b>#{request_id}</b> received\n\n"
        f"⭐ Stars: <b>{stars_amount:,}</b>\n"
        f"💎 For: <b>{format_ton(ton_amount)} TON</b>\n"
        f"👛 Wallet: <code>{wallet}</code>\n\n"
        f"⏳ Your request is now awaiting admin review, we'll notify you once it's decided."
    )

    display_name = get_display_name(user_id)
    notify_admin(
        f"🆕 <b>New exchange request</b> #{request_id}\n\n"
        f"👤 User: {display_name} (<code>{user_id}</code>)\n"
        f"⭐ Stars: <b>{stars_amount:,}</b>\n"
        f"💎 Equivalent: <b>{format_ton(ton_amount)} TON</b>\n"
        f"👛 Wallet: <code>{wallet}</code>\n\n"
        f"Accept or reject this request:",
        admin_decision_keyboard(request_id)
    )


# ───────────────────────── Webhook ─────────────────────────
@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        logger.warning("⚠️ Webhook call rejected: bad or missing secret token")
        return jsonify({"ok": False}), 403

    if not check_rate_limit("webhook"):
        return jsonify({"ok": True}), 429

    update = request.get_json(force=True, silent=True) or {}
    logger.info(f"📩 Update received: {json.dumps(update)[:300]}")

    # Pre-checkout: always approve (real validation happens before the invoice is sent)
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

        # Successful payment (Stars actually charged)
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
                        f"💛 Successfully received <b>{amount:,}⭐</b> for request #{request_id}\n\n"
                        f"<b>{format_ton(req['ton_amount'])} TON</b> will be sent to your wallet shortly 🙏"
                    )
                    display_name = get_display_name(user_id)
                    notify_admin(
                        f"💰 <b>Payment received</b> - request #{request_id}\n\n"
                        f"👤 {display_name} (<code>{user_id}</code>)\n"
                        f"⭐ {amount:,} received\n"
                        f"👛 Send <b>{format_ton(req['ton_amount'])} TON</b> to:\n<code>{req['ton_wallet']}</code>",
                        admin_mark_done_keyboard(request_id)
                    )
                else:
                    logger.error(f"successful_payment: exchange request {request_id} not in expected state")
                    send_message(chat_id, "⚠️ Payment received, but something went wrong updating your request. Please contact support.")
            return jsonify({"ok": True})

        # ───── Waiting for wallet address ─────
        state = get_state(user_id)

        if state == "waiting_wallet" and text and not text.startswith("/"):
            if not is_valid_wallet(text):
                send_message(chat_id, "❌ Invalid wallet address. Make sure you copied it correctly from your wallet app (Tonkeeper, Tonhub, etc.) and send it again.")
                return jsonify({"ok": True})
            set_pending_wallet(user_id, text)
            set_state(user_id, None)
            send_message(chat_id, "👛 Wallet address received ✅\n\nNow choose how many stars you'd like to exchange:", exchange_amount_keyboard())
            return jsonify({"ok": True})

        # ───── Waiting for custom stars amount ─────
        if state == "waiting_stars_custom" and text and not text.startswith("/"):
            wallet = pop_pending_wallet(user_id)
            if not wallet:
                set_state(user_id, None)
                send_message(chat_id, "⚠️ Your session expired, please start again by tapping 🔄 Exchange Stars for TON")
                return jsonify({"ok": True})
            digits = ''.join(filter(str.isdigit, text))
            if not digits:
                send_message(chat_id, "❌ Please enter a valid number, e.g. 3000")
                set_pending_wallet(user_id, wallet)
                return jsonify({"ok": True})
            stars_amount = int(digits)
            set_state(user_id, None)
            start_exchange_request(user_id, chat_id, stars_amount, wallet)
            return jsonify({"ok": True})

        # ───── Commands ─────
        if text.startswith("/start"):
            send_message(
                chat_id,
                "👋 Welcome to the <b>Stars ↔ TON Exchange Bot</b> 💎\n\n"
                f"💱 Current exchange rate: <b>{STARS_PER_TON:,} ⭐ = 1 TON</b>\n\n"
                "How it works:\n"
                "1️⃣ Submit an exchange request (with your wallet address)\n"
                "2️⃣ The admin reviews and either accepts or rejects it\n"
                "3️⃣ If accepted, you pay the Stars through Telegram\n"
                "4️⃣ We send the equivalent TON to your wallet\n\n"
                "Choose an option below:",
                main_keyboard()
            )

        elif text in ("/myrequests", "/requests"):
            send_message(chat_id, build_my_requests_text(user_id))

        elif text in ("/help",):
            send_message(
                chat_id,
                "❓ <b>About this bot</b>\n\n"
                f"This bot exchanges Telegram Stars for TON at a fixed rate:\n"
                f"<b>{STARS_PER_TON:,} ⭐ = 1 TON</b>\n\n"
                "Every request goes through manual admin review before payment, and after "
                "payment the TON is sent manually to your wallet.\n\n"
                "Commands:\n"
                "/start - Main menu\n"
                "/myrequests - View your past requests"
            )

        # Admin commands
        elif text.startswith("/admin") and str(user_id) == str(ADMIN_CHAT_ID):
            stats = get_admin_stats()
            lines = ["🛠 <b>Admin Panel</b>\n"]
            for status_key in ("pending", "accepted", "paid", "completed", "rejected"):
                s = stats.get(status_key, {"count": 0, "stars": 0, "ton": 0})
                lines.append(f"{status_label(status_key)}: {s['count']} requests / {s['stars']:,}⭐ / {format_ton(s['ton'])} TON")
            send_message(chat_id, "\n".join(lines))

        elif text == "/clearcache" and str(user_id) == str(ADMIN_CHAT_ID):
            clear_cache()
            send_message(chat_id, "✅ Cache cleared")

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
                "👛 Send the TON wallet address you'd like to receive your funds on\n\n"
                "Example: <code>UQAbCdEf...</code>\n\n"
                "⚠️ Please double-check the address, the bot is not responsible for transfers to wrong addresses."
            )

        elif data_key == "my_requests":
            answer_callback(callback_id, "")
            send_message(chat_id, build_my_requests_text(user_id))

        elif data_key == "help":
            answer_callback(callback_id, "")
            send_message(
                chat_id,
                "❓ <b>About this bot</b>\n\n"
                f"This bot exchanges Telegram Stars for TON at a fixed rate: <b>{STARS_PER_TON:,} ⭐ = 1 TON</b>\n\n"
                "Every request goes through manual review before payment, and after payment "
                "the TON is transferred manually to your wallet."
            )

        elif data_key == "back_main":
            answer_callback(callback_id, "")
            send_message(chat_id, "Main menu:", main_keyboard())

        elif data_key == "exchange_custom":
            wallet = None
            with wallet_lock:
                wallet = pending_wallet.get(user_id)
            if not wallet:
                answer_callback(callback_id, "Please enter your wallet address first", show_alert=True)
                set_state(user_id, "waiting_wallet")
                send_message(chat_id, "👛 Please send your TON wallet address first:")
                return jsonify({"ok": True})
            answer_callback(callback_id, "")
            set_state(user_id, "waiting_stars_custom")
            send_message(chat_id, f"📝 Type the number of stars you'd like to exchange (digits only)\nMinimum: {MIN_EXCHANGE_STARS:,} ⭐")

        elif data_key.startswith("exchange_amt_"):
            wallet = pop_pending_wallet(user_id)
            if not wallet:
                answer_callback(callback_id, "Please enter your wallet address first", show_alert=True)
                set_state(user_id, "waiting_wallet")
                send_message(chat_id, "👛 Please send your TON wallet address first:")
                return jsonify({"ok": True})
            try:
                amount = int(data_key.split("_")[2])
            except Exception:
                answer_callback(callback_id, "Error", show_alert=True)
                return jsonify({"ok": True})
            start_exchange_request(user_id, chat_id, amount, wallet, callback_id=callback_id)

        # ───── Admin decisions ─────
        elif data_key.startswith("exchange_accept_") and str(user_id) == str(ADMIN_CHAT_ID):
            try:
                request_id = int(data_key.split("_")[2])
            except Exception:
                answer_callback(callback_id, "Error", show_alert=True)
                return jsonify({"ok": True})
            req = get_exchange_request(request_id)
            if not req or req["status"] != "pending":
                answer_callback(callback_id, "This request can no longer be accepted", show_alert=True)
                return jsonify({"ok": True})
            update_exchange_status(request_id, "accepted")
            sent = send_invoice(
                req["user_id"], req["stars_amount"],
                f"Exchange {req['stars_amount']}⭐",
                f"For {format_ton(req['ton_amount'])} TON",
                f"exchange_{request_id}"
            )
            if sent:
                answer_callback(callback_id, "✅ Accepted, payment invoice sent to the user")
                send_message(req["user_id"], f"✅ Your request #{request_id} was accepted!\nYou'll now receive a payment request for {req['stars_amount']:,}⭐, complete it to finish the exchange.")
            else:
                answer_callback(callback_id, "Accepted, but sending the invoice failed", show_alert=True)

        elif data_key.startswith("exchange_reject_") and str(user_id) == str(ADMIN_CHAT_ID):
            try:
                request_id = int(data_key.split("_")[2])
            except Exception:
                answer_callback(callback_id, "Error", show_alert=True)
                return jsonify({"ok": True})
            req = get_exchange_request(request_id)
            if not req or req["status"] != "pending":
                answer_callback(callback_id, "This request can no longer be rejected", show_alert=True)
                return jsonify({"ok": True})
            update_exchange_status(request_id, "rejected")
            answer_callback(callback_id, "❌ Request rejected")
            send_message(req["user_id"], f"❌ Sorry, your exchange request #{request_id} was rejected.\nYou're welcome to submit a new one if you'd like.")

        elif data_key.startswith("exchange_done_") and str(user_id) == str(ADMIN_CHAT_ID):
            try:
                request_id = int(data_key.split("_")[2])
            except Exception:
                answer_callback(callback_id, "Error", show_alert=True)
                return jsonify({"ok": True})
            req = get_exchange_request(request_id)
            if not req or req["status"] != "paid":
                answer_callback(callback_id, "This request cannot be completed right now", show_alert=True)
                return jsonify({"ok": True})
            update_exchange_status(request_id, "completed", touch_completed=True)
            answer_callback(callback_id, "✅ Marked as completed")
            send_message(req["user_id"], f"🎉 <b>{format_ton(req['ton_amount'])} TON</b> has been sent to your wallet!\nThanks for using the bot 💎")

    return jsonify({"ok": True})


# ───────────────────────── Health Check ─────────────────────────
@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": datetime.now().isoformat(),
        "rate": f"{STARS_PER_TON} stars = 1 TON",
        "bot_token_set": bool(BOT_TOKEN),
        "database_url_set": bool(DATABASE_URL),
        "admin_chat_id_set": bool(ADMIN_CHAT_ID),
        "public_url": PUBLIC_URL or None,
    })


# Diagnostic route: hit this in a browser after deploying to see whether Telegram
# actually has a webhook registered for this bot, and what URL it's pointing at.
@app.route("/webhook_info")
def webhook_info():
    if not BOT_TOKEN:
        return jsonify({"error": "BOT_TOKEN not set"}), 500
    try:
        r = requests.get(f"{TELEGRAM_API}/getWebhookInfo", timeout=REQUEST_TIMEOUT)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Manually (re)register the webhook without redeploying, useful if PUBLIC_URL
# changed or the automatic registration at startup failed.
@app.route("/set_webhook")
def set_webhook_route():
    ok = set_webhook()
    return jsonify({"ok": ok, "public_url": PUBLIC_URL})


# ───────────────────────── Run ─────────────────────────
if __name__ == "__main__":
    init_db()
    set_webhook()
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"🚀 Bot running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
