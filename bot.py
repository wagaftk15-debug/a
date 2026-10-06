import os
import re
import io
import csv
import json
import time
import html
import logging
import statistics
from threading import Lock
from collections import defaultdict
from contextlib import contextmanager

import requests
import psycopg2
from psycopg2 import pool
from flask import Flask, request, jsonify

# ───────────────────────── Logging ─────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MahrBot")

app = Flask(__name__)

# ───────────────────────── Config ─────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")  # رقم حساب الأدمن

PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")
if not PUBLIC_URL and os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
    PUBLIC_URL = f"https://{os.environ['RAILWAY_PUBLIC_DOMAIN']}"

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

REQUEST_TIMEOUT = 10
MIN_AMOUNT = 1
MAX_AMOUNT = 1_000_000_000_000  # تريليون كحد أعلى لتجنب الإدخالات العبثية

QUESTION = "💍 <b>كم تريدين مهراً لكِ؟</b>\n\nاكتبي الرقم فقط، مثال: <code>10000000</code> أو <code>10 مليون</code>"

# ───────────────────────── Rate limit (per user) ─────────────────────────
rate_limit = defaultdict(list)
rate_lock = Lock()
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 20


def check_rate_limit(key) -> bool:
    now = time.time()
    with rate_lock:
        rate_limit[key] = [t for t in rate_limit[key] if now - t < RATE_LIMIT_WINDOW]
        if len(rate_limit[key]) >= RATE_LIMIT_MAX:
            return False
        rate_limit[key].append(now)
        return True


# ───────────────────────── Database ─────────────────────────
db_pool = None
pool_lock = Lock()


def init_pool() -> bool:
    global db_pool
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set")
        return False
    with pool_lock:
        if db_pool is not None:
            return True
        try:
            db_pool = pool.SimpleConnectionPool(
                1, 10, DATABASE_URL,
                connect_timeout=8,
                keepalives=1, keepalives_idle=30,
                keepalives_interval=10, keepalives_count=5,
            )
            logger.info("✅ Connection pool ready")
            return True
        except Exception as e:
            logger.error(f"❌ Pool error: {e}")
            return False


@contextmanager
def db_cursor():
    """يعطي cursor ويعمل commit/rollback ويرجّع الاتصال للـ pool تلقائياً."""
    if db_pool is None and not init_pool():
        raise RuntimeError("Database pool unavailable")
    conn = db_pool.getconn()
    broken = False
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
        cur.close()
    except psycopg2.OperationalError:
        broken = True
        raise
    except Exception:
        try:
            conn.rollback()
        except Exception:
            broken = True
        raise
    finally:
        db_pool.putconn(conn, close=broken)


def init_db():
    try:
        with db_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mahr_users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    state TEXT,
                    mahr_amount BIGINT,
                    raw_answer TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    answered_at TIMESTAMP
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_mahr_amount ON mahr_users(mahr_amount)")
        logger.info("✅ Database ready")
    except Exception as e:
        logger.error(f"❌ init_db error: {e}")


def upsert_user(user_id, username, first_name):
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO mahr_users (user_id, username, first_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET username = EXCLUDED.username, first_name = EXCLUDED.first_name
        """, (user_id, username or "", first_name or ""))


def set_state(user_id, state):
    with db_cursor() as cur:
        cur.execute("UPDATE mahr_users SET state = %s WHERE user_id = %s", (state, user_id))


def get_user(user_id):
    with db_cursor() as cur:
        cur.execute("SELECT state, mahr_amount FROM mahr_users WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
    return {"state": row[0], "amount": row[1]} if row else None


def save_answer(user_id, amount, raw_text):
    with db_cursor() as cur:
        cur.execute("""
            UPDATE mahr_users
            SET mahr_amount = %s, raw_answer = %s, answered_at = NOW(), state = NULL
            WHERE user_id = %s
        """, (amount, raw_text[:200], user_id))


def get_stats():
    with db_cursor() as cur:
        cur.execute("SELECT mahr_amount FROM mahr_users WHERE mahr_amount IS NOT NULL")
        amounts = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) FROM mahr_users")
        total_users = cur.fetchone()[0]
    return total_users, amounts


def get_all_answers():
    with db_cursor() as cur:
        cur.execute("""
            SELECT user_id, username, first_name, mahr_amount, raw_answer, answered_at
            FROM mahr_users WHERE mahr_amount IS NOT NULL
            ORDER BY answered_at DESC
        """)
        return cur.fetchall()


# ───────────────────────── Helpers ─────────────────────────
_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")


def parse_amount(text: str):
    """يحوّل نص مثل '10 مليون' أو '١٠٠٠٠٠٠٠' أو '10,000,000' إلى رقم صحيح. يرجع None إذا فشل."""
    t = text.translate(_DIGITS).lower()
    t = t.replace(",", "").replace("٬", "").replace("٫", ".").replace("،", "")
    m = re.search(r"\d+(?:\.\d+)?", t)
    if not m:
        return None
    number = float(m.group())
    multiplier = 1
    if re.search(r"مليار|بليون|billion", t):
        multiplier = 1_000_000_000
    elif re.search(r"مليون|ملايين|million", t):
        multiplier = 1_000_000
    elif re.search(r"ألف|الف|آلاف|الاف|thousand", t):
        multiplier = 1_000
    value = int(round(number * multiplier))
    if value < MIN_AMOUNT or value > MAX_AMOUNT:
        return None
    return value


def fmt(n) -> str:
    return f"{int(n):,}"


def esc(s) -> str:
    return html.escape(str(s or ""))


# ───────────────────────── Telegram API ─────────────────────────
def send_message(chat_id, text, reply_markup=None):
    if not chat_id or not text:
        return False
    try:
        payload = {
            "chat_id": int(chat_id),
            "text": text[:4096],
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUEST_TIMEOUT)
        return r.json().get("ok", False)
    except Exception as e:
        logger.error(f"send_message error: {e}")
        return False


def answer_callback(callback_id, text=""):
    try:
        requests.post(
            f"{TELEGRAM_API}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text[:200]},
            timeout=REQUEST_TIMEOUT,
        )
    except Exception as e:
        logger.error(f"answer_callback error: {e}")


def send_document(chat_id, filename, content_bytes, caption=""):
    try:
        requests.post(
            f"{TELEGRAM_API}/sendDocument",
            data={"chat_id": int(chat_id), "caption": caption},
            files={"document": (filename, content_bytes)},
            timeout=30,
        )
    except Exception as e:
        logger.error(f"send_document error: {e}")


def set_webhook() -> bool:
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN is not set")
        return False
    if not PUBLIC_URL:
        logger.warning("⚠️ PUBLIC_URL / RAILWAY_PUBLIC_DOMAIN not set - webhook not registered")
        return False
    url = f"{PUBLIC_URL}/webhook/{BOT_TOKEN}"
    try:
        payload = {"url": url, "allowed_updates": ["message", "callback_query"]}
        if WEBHOOK_SECRET:
            payload["secret_token"] = WEBHOOK_SECRET
        r = requests.post(f"{TELEGRAM_API}/setWebhook", json=payload, timeout=REQUEST_TIMEOUT)
        ok = r.json().get("ok", False)
        logger.info(f"{'✅ Webhook registered' if ok else '❌ setWebhook failed'}: {r.text[:200]}")
        return ok
    except Exception as e:
        logger.error(f"❌ setWebhook error: {e}")
        return False


def is_admin(user_id) -> bool:
    return bool(ADMIN_CHAT_ID) and str(user_id) == str(ADMIN_CHAT_ID)


# ───────────────────────── Keyboards ─────────────────────────
def edit_keyboard():
    return {"inline_keyboard": [[{"text": "✏️ تعديل إجابتي", "callback_data": "edit_answer"}]]}


# ───────────────────────── Bot logic ─────────────────────────
def ask_question(chat_id, user_id):
    set_state(user_id, "waiting_amount")
    send_message(chat_id, QUESTION)


def handle_amount_answer(chat_id, user_id, text):
    amount = parse_amount(text)
    if amount is None:
        send_message(
            chat_id,
            "❌ لم أفهم الرقم، اكتبيه بالأرقام فقط من فضلك.\nمثال: <code>10000000</code> أو <code>10 مليون</code>",
        )
        return
    save_answer(user_id, amount, text)
    send_message(
        chat_id,
        f"✅ تم حفظ إجابتك\n\n💍 المهر المطلوب: <b>{fmt(amount)}</b>\n\nشكراً لكِ 🌸",
        edit_keyboard(),
    )
    # إشعار للأدمن
    if ADMIN_CHAT_ID and not is_admin(user_id):
        send_message(ADMIN_CHAT_ID, f"🆕 إجابة جديدة: <b>{fmt(amount)}</b>")


def handle_admin_stats(chat_id):
    total_users, amounts = get_stats()
    if not amounts:
        send_message(chat_id, f"🛠 <b>لوحة الأدمن</b>\n\nالمستخدمات: {total_users}\nلا توجد إجابات بعد.")
        return
    send_message(
        chat_id,
        "🛠 <b>إحصائيات</b>\n\n"
        f"👥 إجمالي المستخدمات: <b>{total_users}</b>\n"
        f"📝 عدد الإجابات: <b>{len(amounts)}</b>\n"
        f"📊 المتوسط: <b>{fmt(statistics.mean(amounts))}</b>\n"
        f"📍 الوسيط: <b>{fmt(statistics.median(amounts))}</b>\n"
        f"⬇️ الأقل: <b>{fmt(min(amounts))}</b>\n"
        f"⬆️ الأعلى: <b>{fmt(max(amounts))}</b>",
    )


def handle_admin_export(chat_id):
    rows = get_all_answers()
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["user_id", "username", "first_name", "mahr_amount", "raw_answer", "answered_at"])
    for r in rows:
        writer.writerow(r)
    # utf-8-sig ليفتح العربي صح في Excel
    send_document(chat_id, "mahr_answers.csv", buf.getvalue().encode("utf-8-sig"), f"{len(rows)} إجابة")


def handle_message(msg):
    chat = msg.get("chat", {})
    chat_id = chat.get("id")
    sender = msg.get("from", {})
    user_id = sender.get("id")
    text = (msg.get("text") or "").strip()

    if not user_id or not chat_id or chat.get("type") != "private":
        return
    if not check_rate_limit(user_id):
        return

    upsert_user(user_id, sender.get("username"), sender.get("first_name"))

    if text.startswith("/start"):
        send_message(chat_id, "أهلاً بكِ 🌸")
        ask_question(chat_id, user_id)
        return

    if text == "/myanswer":
        u = get_user(user_id)
        if u and u["amount"]:
            send_message(chat_id, f"💍 إجابتك المحفوظة: <b>{fmt(u['amount'])}</b>", edit_keyboard())
        else:
            send_message(chat_id, "لم تجيبي بعد. اضغطي /start للبدء.")
        return

    if text == "/stats" and is_admin(user_id):
        handle_admin_stats(chat_id)
        return

    if text == "/export" and is_admin(user_id):
        handle_admin_export(chat_id)
        return

    if text.startswith("/"):
        return

    user = get_user(user_id)
    if user and user["state"] == "waiting_amount":
        handle_amount_answer(chat_id, user_id, text)
    else:
        send_message(chat_id, "اضغطي /start للإجابة على السؤال، أو /myanswer لعرض إجابتك.")


def handle_callback(cq):
    sender = cq.get("from", {})
    user_id = sender.get("id")
    chat_id = cq.get("message", {}).get("chat", {}).get("id")
    answer_callback(cq.get("id"))
    if not user_id or not chat_id:
        return
    if cq.get("data") == "edit_answer":
        upsert_user(user_id, sender.get("username"), sender.get("first_name"))
        ask_question(chat_id, user_id)


# ───────────────────────── Webhook ─────────────────────────
@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return jsonify({"ok": False}), 403

    update = request.get_json(force=True, silent=True) or {}
    try:
        if "message" in update:
            handle_message(update["message"])
        elif "callback_query" in update:
            handle_callback(update["callback_query"])
    except Exception as e:
        # نرجّع 200 دائماً حتى لا يعيد تلقرام إرسال نفس التحديث بلا نهاية
        logger.exception(f"update handling error: {e}")
    return jsonify({"ok": True})


# ───────────────────────── Health / diagnostics ─────────────────────────
@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "bot_token_set": bool(BOT_TOKEN),
        "database_url_set": bool(DATABASE_URL),
        "admin_chat_id_set": bool(ADMIN_CHAT_ID),
        "public_url": PUBLIC_URL or None,
    })


@app.route("/webhook_info")
def webhook_info():
    if not BOT_TOKEN:
        return jsonify({"error": "BOT_TOKEN not set"}), 500
    try:
        r = requests.get(f"{TELEGRAM_API}/getWebhookInfo", timeout=REQUEST_TIMEOUT)
        info = r.json()
        # لا نعرض التوكن في الرابط
        if isinstance(info.get("result"), dict) and info["result"].get("url"):
            info["result"]["url"] = info["result"]["url"].replace(BOT_TOKEN, "<TOKEN>")
        return jsonify(info)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ───────────────────────── Startup ─────────────────────────
# يعمل مع gunicorn أيضاً (وليس فقط python app.py)
init_db()
set_webhook()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"🚀 Bot running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
