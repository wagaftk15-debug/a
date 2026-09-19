import os
import json
import requests
import psycopg2
from psycopg2 import pool
from flask import Flask, request, jsonify, make_response
from datetime import datetime, timedelta
from threading import Lock
import time
import gzip
import logging
from io import BytesIO
from collections import defaultdict

# ───────────────────────── إعدادات اللوجينج ─────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("NisaaBot")

app = Flask(__name__)

# ───────────────────────── الإعدادات ─────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")
APP_URL = os.environ.get("APP_URL", "").rstrip("/")
RAILWAY_DOMAIN = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()

DONATION_AMOUNTS = [100, 200, 500, 1000, 2000]
DAILY_POINTS = 100
REFERRAL_POINTS = 50           # نقاط الإحالة

# مستويات المهور
MAHR_LEVELS = [
    (0, "بلا متطلبات محددة 💚"),
    (500, "متطلبات بسيطة 💛"),
    (2000, "متطلبات معقولة 🧡"),
    (5000, "متطلبات عالية 🔴"),
    (15000, "متطلبات فاخرة 💎"),
]

# ───────────────────────── Connection Pool ─────────────────────────
db_pool = None
pool_lock = Lock()
MAX_RETRIES = 3
RETRY_DELAY = 0.4
REQUEST_TIMEOUT = 10

# ───────────────────────── Cache + Rate Limit ─────────────────────────
cache = {}
cache_lock = Lock()
CACHE_TTL = {
    'stats': 30,
    'leaderboard': 45,
    'user_points': 10,
    'count': 40,
    'donations_stats': 30,
    'mahr_stats': 40,
}

# Rate limiting بسيط (في الذاكرة)
rate_limit = defaultdict(list)
rate_lock = Lock()
RATE_LIMIT_WINDOW = 60      # ثانية
RATE_LIMIT_MAX = 25         # طلبات

# حالات المستخدمات (waiting mahr input)
user_states = {}
state_lock = Lock()

def check_rate_limit(key: str) -> bool:
    """يرجع True إذا مسموح"""
    now = time.time()
    with rate_lock:
        rate_limit[key] = [t for t in rate_limit[key] if now - t < RATE_LIMIT_WINDOW]
        if len(rate_limit[key]) >= RATE_LIMIT_MAX:
            return False
        rate_limit[key].append(now)
        return True

# ───────────────────────── دوال مساعدة ─────────────────────────
def get_site_url():
    if APP_URL:
        return APP_URL
    if RAILWAY_DOMAIN:
        return f"https://{RAILWAY_DOMAIN}"
    return ""

def get_mahr_level(amount: int) -> str:
    level_name = MAHR_LEVELS[0][1]
    for threshold, name in MAHR_LEVELS:
        if amount >= threshold:
            level_name = name
    return level_name

def mask_name(name: str) -> str:
    if not name:
        return "****"
    name = str(name).strip()[:18]
    if len(name) <= 2:
        return name[0] + "*"
    return name[0] + "*" * (len(name) - 2) + name[-1]

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
    except:
        try:
            conn.close()
        except:
            pass

def get_cache(key):
    with cache_lock:
        if key in cache:
            data, timestamp = cache[key]
            ttl = CACHE_TTL.get(key.split('_')[0], 60)
            if data is not None and datetime.now() - timestamp < timedelta(seconds=ttl):
                return data
            try:
                del cache[key]
            except:
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
                except:
                    pass

def init_db():
    if not DATABASE_URL:
        return
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS women (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                referred_by BIGINT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS registrations (
                user_id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mahr_requirements (
                user_id BIGINT PRIMARY KEY,
                mahr_amount INTEGER NOT NULL DEFAULT 0,
                mahr_notes TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS donations (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                amount INTEGER NOT NULL,
                telegram_payment_charge_id TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contacts_sent (
                id SERIAL PRIMARY KEY,
                sender_id BIGINT NOT NULL,
                recipient_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_points (
                user_id BIGINT PRIMARY KEY,
                total_points INTEGER NOT NULL DEFAULT 0,
                last_claim_date DATE
            )
        """)

        # Indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mahr_amount ON mahr_requirements(mahr_amount DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_registrations_created ON registrations(created_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_donations_user ON donations(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_sent ON contacts_sent(sender_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_women_referred ON women(referred_by)")

        conn.commit()
        cur.close()
        logger.info("✅ قاعدة البيانات جاهزة")
    except Exception as e:
        logger.error(f"❌ خطأ في DB: {e}")
    finally:
        return_connection(conn)

# ───────────────────────── Woman Functions ─────────────────────────
def upsert_woman(user_id, username=None, first_name=None, referred_by=None):
    if not user_id:
        return
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO women (user_id, username, first_name, referred_by, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET username = COALESCE(EXCLUDED.username, women.username),
                first_name = COALESCE(EXCLUDED.first_name, women.first_name),
                updated_at = NOW()
        """, (user_id, username or '', first_name or '', referred_by))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error(f"upsert_woman error: {e}")
    finally:
        return_connection(conn)

def get_count():
    cached = get_cache('count')
    if cached is not None:
        return cached
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM registrations")
        count = cur.fetchone()[0] or 0
        cur.close()
        set_cache('count', count)
        return count
    except Exception as e:
        logger.error(f"get_count error: {e}")
        return 0
    finally:
        return_connection(conn)

def is_registered(user_id):
    if not user_id:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM registrations WHERE user_id = %s LIMIT 1", (user_id,))
        result = cur.fetchone() is not None
        cur.close()
        return result
    except Exception as e:
        logger.error(f"is_registered error: {e}")
        return False
    finally:
        return_connection(conn)

def register_woman(user_id, referred_by=None):
    if not user_id:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO registrations (user_id) VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (user_id,))
        added = cur.rowcount > 0
        conn.commit()
        cur.close()

        if added:
            clear_cache('count')
            clear_cache('stats')
            # إعطاء نقاط للإحالة
            if referred_by and referred_by != user_id:
                add_points(referred_by, REFERRAL_POINTS)
                send_message(referred_by, f"🎉 حصلت على <b>{REFERRAL_POINTS}</b> نقطة إحالة من أختك الجديدة!")
        return added
    except Exception as e:
        logger.error(f"register_woman error: {e}")
        return False
    finally:
        return_connection(conn)

def unregister_woman(user_id):
    if not user_id:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM registrations WHERE user_id = %s", (user_id,))
        removed = cur.rowcount > 0
        conn.commit()
        cur.close()
        if removed:
            clear_cache('count')
            clear_cache('stats')
        return removed
    except Exception as e:
        logger.error(f"unregister_woman error: {e}")
        return False
    finally:
        return_connection(conn)

# ───────────────────────── Mahr Functions ─────────────────────────
def set_mahr(user_id, amount, notes=None):
    if not user_id or amount < 0:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO mahr_requirements (user_id, mahr_amount, mahr_notes)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET mahr_amount = EXCLUDED.mahr_amount,
                mahr_notes = EXCLUDED.mahr_notes,
                updated_at = NOW()
        """, (user_id, amount, notes[:500] if notes else None))
        conn.commit()
        cur.close()
        clear_cache('mahr_stats')
        clear_cache('leaderboard')
        return True
    except Exception as e:
        logger.error(f"set_mahr error: {e}")
        return False
    finally:
        return_connection(conn)

def get_mahr(user_id):
    if not user_id:
        return 0, None
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT mahr_amount, mahr_notes FROM mahr_requirements WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
        if row:
            return row[0] or 0, row[1]
        return 0, None
    except Exception as e:
        logger.error(f"get_mahr error: {e}")
        return 0, None
    finally:
        return_connection(conn)

def get_leaderboard(limit=10):
    """الحصول على قائمة النساء المسجلات مع متطلبات المهر"""
    cached = get_cache('leaderboard')
    if cached is not None:
        return cached
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                m.user_id,
                COALESCE(NULLIF(w.username, ''), NULLIF(w.first_name, ''), 'مستخدمة') AS display_name,
                m.mahr_amount,
                m.mahr_notes
            FROM mahr_requirements m
            LEFT JOIN women w ON w.user_id = m.user_id
            WHERE m.user_id IN (SELECT user_id FROM registrations)
            ORDER BY m.mahr_amount DESC, m.user_id ASC
            LIMIT %s
        """, (min(limit, 20),))
        rows = cur.fetchall()
        cur.close()
        result = [
            {
                "user_id": r[0],
                "name": r[1] or 'مستخدمة',
                "mahr_amount": r[2] or 0,
                "mahr_notes": r[3]
            }
            for r in rows
        ]
        if result:
            set_cache('leaderboard', result)
        return result
    except Exception as e:
        logger.error(f"get_leaderboard error: {e}")
        return []
    finally:
        return_connection(conn)

# ───────────────────────── Points ─────────────────────────
def add_points(user_id, amount):
    if not user_id or amount <= 0:
        return 0
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_points (user_id, total_points)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET total_points = user_points.total_points + %s
            RETURNING total_points
        """, (user_id, amount, amount))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        points = row[0] if row else 0
        clear_cache(f'user_points_{user_id}')
        return points
    except Exception as e:
        logger.error(f"add_points error: {e}")
        return 0
    finally:
        return_connection(conn)

def get_user_points(user_id):
    if not user_id:
        return 0
    key = f'user_points_{user_id}'
    cached = get_cache(key)
    if cached is not None:
        return cached
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT total_points FROM user_points WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
        points = row[0] if row else 0
        set_cache(key, points)
        return points
    except Exception as e:
        logger.error(f"get_user_points error: {e}")
        return 0
    finally:
        return_connection(conn)

# ───────────────────────── Donations ─────────────────────────
def record_donation(user_id, amount, charge_id):
    if not user_id or amount <= 0:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO donations (user_id, amount, telegram_payment_charge_id)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (user_id, amount, charge_id or None))
        added = cur.rowcount > 0
        conn.commit()
        cur.close()
        if added:
            clear_cache('donations_stats')
            clear_cache('stats')
        return added
    except Exception as e:
        logger.error(f"record_donation error: {e}")
        return False
    finally:
        return_connection(conn)

def get_donations_stats():
    cached = get_cache('donations_stats')
    if cached is not None:
        return cached
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM donations")
        row = cur.fetchone()
        cur.close()
        result = (row[0] or 0, row[1] or 0) if row else (0, 0)
        set_cache('donations_stats', result)
        return result
    except Exception as e:
        logger.error(f"get_donations_stats error: {e}")
        return (0, 0)
    finally:
        return_connection(conn)

# ───────────────────────── Telegram ─────────────────────────
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
            logger.error(f"Invoice error: {result.get('description')}")
            send_message(chat_id, "⚠️ حصل خطأ في إنشاء الفاتورة، جرب مرة ثانية.")
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

# ───────────────────────── Keyboards ─────────────────────────
def main_keyboard():
    site = get_site_url()
    keyboard = [
        [{"text": "✅ تسجيل رغبتي بالزواج", "callback_data": "want_marry"}],
        [
            {"text": "💍 تحديد المهر", "callback_data": "set_mahr"},
            {"text": "📊 قائمة الأخوات", "callback_data": "show_leaderboard"},
        ],
        [
            {"text": "⭐ دعم البوت", "callback_data": "show_donate"},
            {"text": "❓ المساعدة", "callback_data": "help"},
        ],
    ]
    if site:
        keyboard.append([{"text": "💻 الموقع", "web_app": {"url": site}}])
    keyboard.append([{"text": "❌ إلغاء التسجيل", "callback_data": "unsubscribe"}])
    return {"inline_keyboard": keyboard}

def donation_keyboard():
    return {
        "inline_keyboard": [
            [{"text": f"⭐ {amount}", "callback_data": f"donate_{amount}"}]
            for amount in DONATION_AMOUNTS
        ] + [[{"text": "🔙 رجوع", "callback_data": "back_main"}]]
    }

def mahr_amount_keyboard():
    amounts = [1000, 2500, 5000, 10000, 20000]
    keyboard = []
    for amount in amounts:
        keyboard.append([{"text": f"💍 {amount:,}", "callback_data": f"mahr_amount_{amount}"}])
    keyboard.append([{"text": "📝 مبلغ مخصص", "callback_data": "mahr_custom"}])
    keyboard.append([{"text": "🔙 رجوع", "callback_data": "back_main"}])
    return {"inline_keyboard": keyboard}

def build_leaderboard_text():
    top = get_leaderboard(15)
    if not top:
        return "🙂 لم تسجل أي أخت متطلبات المهر بعد"
    medals = ["🥇", "🥈", "🥉"]
    lines = ["💍 <b>قائمة متطلبات المهر</b>\n"]
    for i, u in enumerate(top):
        medal = medals[i] if i < 3 else f"{i+1}."
        level = get_mahr_level(u['mahr_amount'])
        notes = f" - {u['mahr_notes'][:30]}" if u['mahr_notes'] else ""
        lines.append(f"{medal} {mask_name(u['name'])} → <b>{u['mahr_amount']:,}</b> 💍 {level}{notes}")
    return "\n".join(lines)

def notify_admin(text):
    if ADMIN_CHAT_ID:
        send_message(ADMIN_CHAT_ID, text)

# ───────────────────────── Webhook ─────────────────────────
@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    if not check_rate_limit("webhook"):
        return jsonify({"ok": True}), 429

    update = request.get_json(force=True, silent=True) or {}

    # Pre-checkout
    if "pre_checkout_query" in update:
        pcq = update["pre_checkout_query"]
        answer_pre_checkout(pcq["id"], ok=True)
        return jsonify({"ok": True})

    # Message
    if "message" in update:
        msg = update["message"]
        chat_id = msg.get("chat", {}).get("id")
        text = (msg.get("text") or "").strip()
        sender = msg.get("from", {})
        user_id = sender.get("id")

        if user_id:
            upsert_woman(user_id, username=sender.get("username"), first_name=sender.get("first_name"))

        # Successful Payment
        if "successful_payment" in msg:
            sp = msg["successful_payment"]
            amount = sp.get("total_amount", 0)
            charge_id = sp.get("telegram_payment_charge_id", "")
            
            if amount > 0 and user_id:
                success = record_donation(user_id, amount, charge_id)
                if success:
                    send_message(chat_id, f"💛 شكراً لدعمك الكبير!\nتم استلام: <b>{amount}⭐</b>\n\nجزاك الله خيراً 🤲")
                    notify_admin(f"✅ دعم جديد\nالمستخدمة: {user_id}\nالمبلغ: {amount}⭐")
                else:
                    send_message(chat_id, "⚠️ تم الدفع لكن حصل خطأ في التسجيل.")
            return jsonify({"ok": True})

        # حالة انتظار إدخال المهر
        with state_lock:
            state = user_states.get(user_id)

        if state == "waiting_mahr" and text and not text.startswith("/"):
            try:
                # محاولة استخراج المبلغ من الرسالة
                mahr_amount = int(''.join(filter(str.isdigit, text.split()[0])))
                mahr_notes = text[len(str(mahr_amount)):].strip() or None
                
                if mahr_amount > 0:
                    success = set_mahr(user_id, mahr_amount, mahr_notes)
                    with state_lock:
                        user_states.pop(user_id, None)
                    if success:
                        level = get_mahr_level(mahr_amount)
                        send_message(chat_id, f"✅ تم تسجيل متطلبات المهر بنجاح!\n\n💍 المبلغ: <b>{mahr_amount:,}</b>\n📍 المستوى: {level}\n\nيمكن للأخوات البحث عنك الآن 💕")
                        notify_admin(f"💍 تحديث مهر\nالمستخدمة: {user_id}\nالمبلغ: {mahr_amount:,}\nملاحظات: {mahr_notes or 'بلا'}")
                    else:
                        send_message(chat_id, "❌ حصل خطأ أثناء التسجيل.")
                else:
                    send_message(chat_id, "❌ يجب إدخال رقم صحيح")
            except:
                send_message(chat_id, "❌ صيغة خاطئة. أدخلي الرقم بشكل صحيح مثل: 5000\nأو مع ملاحظات: 5000 بلاش إذا كنت شايفة في")
            return jsonify({"ok": True})

        # أوامر
        if text.startswith("/start"):
            referred_by = None
            parts = text.split()
            if len(parts) > 1 and parts[1].startswith("ref_"):
                try:
                    referred_by = int(parts[1].replace("ref_", ""))
                except:
                    pass
            upsert_woman(user_id, username=sender.get("username"), first_name=sender.get("first_name"), referred_by=referred_by)

            send_message(
                chat_id,
                "👋 أهلاً وسهلاً بك في <b>بوت نساء الزواج</b> 💍\n\n"
                "هنا تقدري تسجلي رغبتك بالزواج وتحددي متطلبات المهر الخاص بك.\n\n"
                "🌟 المميزات:\n"
                "✨ عرض ملفك لآلاف الأخوات\n"
                "💍 تحديد مهرك بكل وضوح\n"
                "📊 قائمة متطلبات المهور المختلفة\n"
                "🤝 دعم المشروع بالتبرع\n\n"
                "اختاري من القائمة:",
                main_keyboard()
            )

        elif text in ("/عدد", "/count"):
            send_message(chat_id, f"<b>عدد الأخوات المسجلات حالياً:</b> {get_count()} 💍")

        elif text in ("/معلوماتي", "/info"):
            mahr_amount, mahr_notes = get_mahr(user_id)
            level = get_mahr_level(mahr_amount)
            registered = "✅ مسجلة" if is_registered(user_id) else "❌ غير مسجلة"
            mahr_text = f"💍 المهر: <b>{mahr_amount:,}</b> ({level})" if mahr_amount > 0 else "❌ لم تحددي المهر بعد"
            send_message(
                chat_id,
                f"👤 <b>معلوماتك:</b>\n\n"
                f"• الحالة: {registered}\n"
                f"• {mahr_text}"
            )

        elif text in ("/قائمة", "/list", "/leaderboard"):
            send_message(chat_id, build_leaderboard_text())

        elif text in ("/انسحب", "/unsubscribe"):
            if is_registered(user_id):
                send_message(chat_id, "متأكدة إنك تبي تلغي التسجيل؟", {
                    "inline_keyboard": [
                        [{"text": "✅ نعم، ألغِ", "callback_data": "confirm_unsubscribe"}],
                        [{"text": "🙅 لا، رجّعي", "callback_data": "cancel_unsubscribe"}],
                    ]
                })
            else:
                send_message(chat_id, "أنتِ أصلاً مش مسجلة 🙂")

        # أوامر الأدمن
        elif text.startswith("/admin") and str(user_id) == str(ADMIN_CHAT_ID):
            count = get_count()
            d_count, d_total = get_donations_stats()
            top = get_leaderboard(5)
            send_message(
                chat_id,
                f"🛠 <b>لوحة الأدمن</b>\n\n"
                f"• المسجلات: {count}\n"
                f"• عدد التبرعات: {d_count}\n"
                f"• مجموع النجوم: {d_total}⭐\n"
                f"• أعلى مهور: {', '.join([str(u['mahr_amount']) for u in top]) if top else 'لا توجد بيانات'}"
            )

        elif text == "/clearcache" and str(user_id) == str(ADMIN_CHAT_ID):
            clear_cache()
            send_message(chat_id, "✅ تم مسح الـ Cache")

    # Callback Query
    elif "callback_query" in update:
        cq = update["callback_query"]
        sender = cq.get("from", {})
        user_id = sender.get("id")
        chat_id = cq.get("message", {}).get("chat", {}).get("id")
        callback_id = cq.get("id")
        data_key = cq.get("data", "")

        upsert_woman(user_id, username=sender.get("username"), first_name=sender.get("first_name"))

        if data_key == "want_marry":
            if is_registered(user_id):
                answer_callback(callback_id, "أنتِ مسجلة بالفعل 😄", show_alert=True)
            else:
                if register_woman(user_id):
                    answer_callback(callback_id, "✅ تم التسجيل!")
                    send_message(chat_id, f"🎉 تم تسجيلك بنجاح!\n\nعدد الأخوات المسجلات: <b>{get_count()}</b> 💍\n\nالآن حددي متطلبات المهر الخاص بك")
                    send_message(chat_id, "الآن، كم متطلبات المهر الخاص بك؟", mahr_amount_keyboard())
                else:
                    answer_callback(callback_id, "أنتِ مسجلة مسبقاً", show_alert=True)

        elif data_key == "set_mahr":
            answer_callback(callback_id, "")
            send_message(chat_id, "اختاري مبلغ المهر أو حددي مبلغ مخصص:", mahr_amount_keyboard())

        elif data_key == "show_leaderboard":
            answer_callback(callback_id, "")
            send_message(chat_id, build_leaderboard_text())

        elif data_key == "show_donate":
            answer_callback(callback_id, "")
            send_message(chat_id, "💛 ادعمي المشروع بنجوم تيليجرام:", donation_keyboard())

        elif data_key == "help":
            answer_callback(callback_id, "")
            send_message(chat_id, 
                "❓ <b>معلومات عن البوت</b>\n\n"
                "البوت يساعدك على:\n"
                "✨ تسجيل رغبتك بالزواج\n"
                "💍 تحديد متطلبات مهرك بوضوح\n"
                "📊 رؤية ملفات الأخوات الأخريات\n"
                "🤝 دعم المشروع\n\n"
                "الأوامر المتاحة:\n"
                "/start - الرئيسية\n"
                "/info - معلوماتك\n"
                "/count - عدد المسجلات\n"
                "/list - قائمة المهور\n"
                "/unsubscribe - إلغاء التسجيل"
            )

        elif data_key == "unsubscribe":
            answer_callback(callback_id, "")
            send_message(chat_id, "متأكدة إنك تبي تلغي التسجيل؟", {
                "inline_keyboard": [
                    [{"text": "✅ نعم", "callback_data": "confirm_unsubscribe"}],
                    [{"text": "🙅 لا", "callback_data": "cancel_unsubscribe"}],
                ]
            })

        elif data_key == "confirm_unsubscribe":
            if unregister_woman(user_id):
                answer_callback(callback_id, "✅ تم إلغاء التسجيل")
                send_message(chat_id, f"👋 تم إلغاء تسجيلك.\n\nإذا تبي تتسجلي مرة ثانية، أكتبي /start")
            else:
                answer_callback(callback_id, "أنتِ مش مسجلة", show_alert=True)

        elif data_key == "cancel_unsubscribe":
            answer_callback(callback_id, "👍 تم التراجع")

        elif data_key == "back_main":
            answer_callback(callback_id, "")
            send_message(chat_id, "القائمة الرئيسية:", main_keyboard())

        elif data_key == "mahr_custom":
            answer_callback(callback_id, "")
            with state_lock:
                user_states[user_id] = "waiting_mahr"
            send_message(chat_id, "كتبي المبلغ الذي تريدينه (بالأرقام فقط)\nمثل: 5000\nأو مع ملاحظات: 5000 إذا كان صادق في نيته")

        elif data_key.startswith("mahr_amount_"):
            try:
                amount = int(data_key.split("_")[2])
                if amount > 0:
                    success = set_mahr(user_id, amount)
                    if success:
                        level = get_mahr_level(amount)
                        answer_callback(callback_id, "✅ تم التسجيل!")
                        send_message(chat_id, f"✅ تم تسجيل متطلبات المهر بنجاح!\n\n💍 المبلغ: <b>{amount:,}</b>\n📍 المستوى: {level}\n\nيمكن للأخوات البحث عنك الآن 💕")
                        notify_admin(f"💍 تحديث مهر\nالمستخدمة: {user_id}\nالمبلغ: {amount:,}")
                    else:
                        answer_callback(callback_id, "خطأ في التسجيل", show_alert=True)
                else:
                    answer_callback(callback_id, "مبلغ غير صالح", show_alert=True)
            except:
                answer_callback(callback_id, "خطأ", show_alert=True)

        elif data_key.startswith("donate_"):
            try:
                amount = int(data_key.split("_")[1])
                if amount in DONATION_AMOUNTS:
                    success = send_invoice(chat_id, amount, f"دعم {amount}⭐", "شكراً لدعمك 💛", f"donate_{amount}")
                    answer_callback(callback_id, "✅ تم إرسال الفاتورة" if success else "❌ فشل", show_alert=not success)
                else:
                    answer_callback(callback_id, "مبلغ غير صالح", show_alert=True)
            except:
                answer_callback(callback_id, "خطأ", show_alert=True)

    return jsonify({"ok": True})

# ───────────────────────── API ─────────────────────────
@app.route("/api/stats", methods=["GET"])
def api_stats():
    try:
        cached = get_cache('stats')
        if cached is not None:
            return jsonify(cached)

        count = get_count()
        donations_count, donations_total = get_donations_stats()
        leaderboard = get_leaderboard(15)

        result = {
            "registered": count,
            "donations_count": donations_count,
            "donations_total": donations_total,
            "leaderboard": leaderboard,
        }
        set_cache('stats', result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)[:80]}), 500

@app.route("/api/woman/info", methods=["POST"])
def api_woman_info():
    data = request.get_json() or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"error": "missing user_id"}), 400
    try:
        mahr_amount, mahr_notes = get_mahr(user_id)
        return jsonify({
            "user_id": user_id,
            "is_registered": is_registered(user_id),
            "mahr_amount": mahr_amount,
            "mahr_level": get_mahr_level(mahr_amount),
            "mahr_notes": mahr_notes,
        })
    except Exception as e:
        return jsonify({"error": str(e)[:80]}), 500

@app.route("/api/register", methods=["POST"])
def api_register():
    data = request.get_json() or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"error": "missing"}), 400
    try:
        result = register_woman(user_id)
        return jsonify({
            "success": result,
            "message": "✅ تم التسجيل!" if result else "مسجلة مسبقاً",
            "count": get_count()
        })
    except Exception as e:
        return jsonify({"error": str(e)[:80]}), 500

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": datetime.now().isoformat(),
        "registered": get_count()
    })

# ───────────────────────── WebApp ─────────────────────────
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>💍 نساء الزواج</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(160deg, #1a0f2e 0%, #2d1b4e 100%);
            color: #f1f5f9;
            min-height: 100vh;
            padding: 16px;
        }
        .container { max-width: 420px; margin: 0 auto; }
        .header { text-align: center; padding: 20px 0 10px; }
        .logo { font-size: 48px; margin-bottom: 6px; }
        h1 {
            font-size: 24px;
            background: linear-gradient(90deg, #ff1493, #db69d6);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .card {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255, 20, 147, 0.2);
            border-radius: 16px;
            padding: 18px;
            margin-bottom: 14px;
        }
        .stats {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            margin-bottom: 16px;
        }
        .stat-box {
            background: rgba(255, 20, 147, 0.1);
            border: 1px solid rgba(255, 20, 147, 0.2);
            border-radius: 12px;
            padding: 14px;
            text-align: center;
        }
        .stat-value { font-size: 22px; font-weight: 700; color: #ff1493; }
        .stat-label { font-size: 12px; opacity: 0.7; margin-top: 4px; }
        .btn {
            display: block;
            width: 100%;
            padding: 14px;
            border: none;
            border-radius: 12px;
            font-size: 15px;
            font-weight: 600;
            cursor: pointer;
            margin-bottom: 10px;
            transition: 0.2s;
        }
        .btn-primary {
            background: linear-gradient(90deg, #ff1493, #db69d6);
            color: white;
        }
        .btn-secondary {
            background: rgba(255, 20, 147, 0.1);
            color: #ffc0cb;
            border: 1px solid rgba(255, 20, 147, 0.3);
        }
        .btn:active { transform: scale(0.98); }
        .leaderboard-item {
            display: flex;
            justify-content: space-between;
            padding: 12px;
            margin-bottom: 8px;
            background: rgba(255, 20, 147, 0.08);
            border-radius: 10px;
            font-size: 14px;
        }
        .user-info {
            text-align: center;
            margin: 12px 0;
            font-size: 14px;
            opacity: 0.9;
            background: rgba(255, 20, 147, 0.1);
            padding: 10px;
            border-radius: 8px;
        }
        .tabs {
            display: flex;
            gap: 8px;
            margin-bottom: 16px;
        }
        .tab {
            flex: 1;
            padding: 10px;
            text-align: center;
            background: rgba(255,255,255,0.05);
            border-radius: 10px;
            font-size: 13px;
            cursor: pointer;
        }
        .tab.active {
            background: linear-gradient(90deg, #ff1493, #db69d6);
        }
        .section { display: none; }
        .section.active { display: block; }
        .loading { text-align: center; padding: 30px; opacity: 0.6; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo">💍</div>
            <h1>نساء الزواج</h1>
        </div>

        <div class="user-info" id="userInfo">جاري التحميل...</div>

        <div class="stats" id="statsBox">
            <div class="stat-box">
                <div class="stat-value" id="regCount">-</div>
                <div class="stat-label">أخت مسجلة</div>
            </div>
            <div class="stat-box">
                <div class="stat-value" id="donTotal">-</div>
                <div class="stat-label">نجوم الدعم</div>
            </div>
        </div>

        <div class="tabs">
            <div class="tab active" onclick="showSection('home')">الرئيسية</div>
            <div class="tab" onclick="showSection('leaderboard')">المهور</div>
            <div class="tab" onclick="showSection('donate')">الدعم</div>
        </div>

        <div id="home" class="section active">
            <div class="card">
                <button class="btn btn-primary" onclick="doRegister()">✅ تسجيل رغبتي</button>
                <button class="btn btn-secondary" onclick="setMahr()">💍 تحديد المهر</button>
                <button class="btn btn-secondary" onclick="Telegram.WebApp.close()">إغلاق</button>
            </div>
        </div>

        <div id="leaderboard" class="section">
            <div class="card" id="lbContent">
                <div class="loading">جاري تحميل قائمة المهور...</div>
            </div>
        </div>

        <div id="donate" class="section">
            <div class="card">
                <p style="text-align:center; margin-bottom:14px; opacity:0.8;">ادعمي المشروع بنجوم تيليجرام ⭐</p>
                <button class="btn btn-primary" onclick="openBotDonate()">فتح خيارات الدعم في البوت</button>
            </div>
        </div>
    </div>

    <script>
        const tg = window.Telegram.WebApp;
        tg.expand();
        tg.ready();

        let userId = null;
        try {
            userId = tg.initDataUnsafe?.user?.id || null;
        } catch(e) {}

        function showSection(id) {
            document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.getElementById(id).classList.add('active');
            event.target.classList.add('active');
            if (id === 'leaderboard') loadLeaderboard();
        }

        async function loadStats() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                document.getElementById('regCount').textContent = data.registered || 0;
                document.getElementById('donTotal').textContent = (data.donations_total || 0) + '⭐';
            } catch(e) {}
        }

        async function loadUser() {
            if (!userId) {
                document.getElementById('userInfo').textContent = 'افتحي التطبيق من داخل تيليجرام';
                return;
            }
            try {
                const res = await fetch('/api/woman/info', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({user_id: userId})
                });
                const data = await res.json();
                const status = data.is_registered ? '✅ مسجلة' : '❌ غير مسجلة';
                const mahr = data.mahr_amount ? `💍 ${data.mahr_amount.toLocaleString('ar-SA')}` : '❌ لم تحددي';
                document.getElementById('userInfo').innerHTML = `${status} | ${mahr}`;
            } catch(e) {
                document.getElementById('userInfo').textContent = 'تعذر تحميل بياناتك';
            }
        }

        async function doRegister() {
            if (!userId) return tg.showAlert('يجب فتح الصفحة من داخل تيليجرام');
            try {
                const res = await fetch('/api/register', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({user_id: userId})
                });
                const data = await res.json();
                tg.showAlert(data.message || 'تم');
                loadStats();
                loadUser();
            } catch(e) {
                tg.showAlert('حدث خطأ');
            }
        }

        function setMahr() {
            tg.showAlert('استخدمي الأمر /set_mahr في البوت لتحديد متطلبات المهر');
        }

        async function loadLeaderboard() {
            const box = document.getElementById('lbContent');
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                const list = data.leaderboard || [];
                if (!list.length) {
                    box.innerHTML = '<div class="loading">لا توجد متطلبات مهور بعد</div>';
                    return;
                }
                let html = '';
                list.forEach((u, i) => {
                    const medal = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : (i+1) + '.';
                    const notes = u.mahr_notes ? `<br><small>${u.mahr_notes}</small>` : '';
                    html += `<div class="leaderboard-item">
                        <div>
                            <span>${medal} ${u.name}</span>
                            ${notes}
                        </div>
                        <span><b>${u.mahr_amount.toLocaleString('ar-SA')}</b> 💍</span>
                    </div>`;
                });
                box.innerHTML = html;
            } catch(e) {
                box.innerHTML = '<div class="loading">فشل التحميل</div>';
            }
        }

        function openBotDonate() {
            tg.close();
        }

        loadStats();
        loadUser();
    </script>
</body>
</html>
"""

@app.route("/")
def index():
    return HTML_TEMPLATE

# ───────────────────────── Gzip ─────────────────────────
@app.after_request
def gzip_response(response):
    if response.content_length and response.content_length < 600:
        return response
    accept = request.headers.get('Accept-Encoding', '')
    if 'gzip' in accept and 'gzip' not in response.headers.get('Content-Encoding', ''):
        try:
            buf = BytesIO()
            with gzip.GzipFile(mode='wb', fileobj=buf) as gz:
                gz.write(response.get_data())
            response.set_data(buf.getvalue())
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Content-Length'] = len(response.get_data())
        except:
            pass
    return response

# ───────────────────────── تشغيل ─────────────────────────
if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"🚀 البوت شغال على المنفذ {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
