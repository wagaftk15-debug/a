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
from io import BytesIO

app = Flask(__name__)

# التوكن والإعدادات
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")

# قيم الدعم والاقتراحات
DONATION_AMOUNTS = [100, 200, 500, 1000]
SUGGESTION_PRICE = 1
DAILY_POINTS = 100

# Connection Pool محسّن - الإصلاح 1 & 2
db_pool = None
pool_lock = Lock()

# Cache محسّن - لا نخزن None - الإصلاح 3
cache = {}
cache_lock = Lock()

CACHE_TTL = {
    'stats': 25,
    'leaderboard': 50,
    'user_points': 8,
    'count': 40,
    'donations_stats': 25,
    'suggestions_count': 40,
}

# الإصلاح 4: تجنب عدم تحميل البيانات
MAX_RETRIES = 3
RETRY_DELAY = 0.3
REQUEST_TIMEOUT = 8


def init_pool():
    """تهيئة connection pool محسّنة - الإصلاح 1"""
    global db_pool
    if not DATABASE_URL:
        print("DATABASE_URL غير موجود")
        return False
    try:
        with pool_lock:
            if db_pool is not None:
                return True
            db_pool = pool.SimpleConnectionPool(
                3, 30, DATABASE_URL, 
                connect_timeout=6,
                keepalives=1,
                keepalives_idle=20,
                keepalives_interval=8,
                keepalives_count=5
            )
        print("✅ Connection Pool جاهز")
        return True
    except Exception as e:
        print(f"❌ خطأ في Pool: {e}")
        return False


def get_connection(retry=0):
    """احصل على اتصال مع إعادة محاولة - الإصلاح 2"""
    global db_pool
    if db_pool is None:
        if not init_pool():
            raise Exception("فشل إنشاء Pool")
    
    try:
        return db_pool.getconn()
    except (pool.PoolError, psycopg2.OperationalError) as e:
        if retry < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return get_connection(retry + 1)
        raise Exception(f"فشل الاتصال: {str(e)[:50]}")


def return_connection(conn, close=False):
    """أرجع الاتصال للـ pool بأمان"""
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
    """احصل على بيانات من cache - لا تعيد None - الإصلاح 3"""
    with cache_lock:
        if key in cache:
            data, timestamp = cache[key]
            ttl = CACHE_TTL.get(key, 60)
            if data is not None and datetime.now() - timestamp < timedelta(seconds=ttl):
                return data
            try:
                del cache[key]
            except:
                pass
    return None


def set_cache(key, data):
    """احفظ بيانات في cache فقط إذا كانت صحيحة - الإصلاح 3"""
    if data is None or (isinstance(data, (int, list)) and not data):
        return
    with cache_lock:
        cache[key] = (data, datetime.now())


def clear_cache(pattern=None):
    """امسح cache بأمان"""
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


# ───────────────────────── قاعدة البيانات ─────────────────────────

def init_db():
    """ينشئ الجداول والـ indexes - الإصلاح 5"""
    if not DATABASE_URL:
        print("DATABASE_URL غير موجود")
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
            CREATE TABLE IF NOT EXISTS registrations (
                user_id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS donations (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                amount INTEGER NOT NULL,
                telegram_payment_charge_id TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS suggestions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                content TEXT,
                telegram_payment_charge_id TEXT,
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

        cur.execute("""
            CREATE TABLE IF NOT EXISTS web_payments (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                type TEXT NOT NULL,
                amount INTEGER NOT NULL,
                telegram_payment_charge_id TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Indexes - الإصلاح 5
        cur.execute("CREATE INDEX IF NOT EXISTS idx_donations_user ON donations(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_suggestions_user ON suggestions(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_points_total ON user_points(total_points DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_registrations_created ON registrations(created_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_donations_date ON donations(created_at DESC)")

        conn.commit()
        cur.close()
        print("✅ قاعدة البيانات جاهزة")
    except Exception as e:
        print(f"❌ خطأ في DB: {e}")
    finally:
        return_connection(conn)


def upsert_user(user_id, username=None, first_name=None):
    """حفظ/تحديث بيانات المستخدم - الإصلاح 6"""
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
        print(f"upsert_user error: {e}")
    finally:
        return_connection(conn)


def get_count():
    """عدد المسجلين مع cache - الإصلاح 7"""
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
        
        if count > 0:
            set_cache('count', count)
        return count
    except Exception as e:
        print(f"get_count error: {e}")
        return 0
    finally:
        return_connection(conn)


def is_registered(user_id):
    """التحقق من التسجيل - الإصلاح 8"""
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
        print(f"is_registered error: {e}")
        return False
    finally:
        return_connection(conn)


def register_user(user_id):
    """تسجيل المستخدم - الإصلاح 9"""
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
        return added
    except Exception as e:
        print(f"register_user error: {e}")
        return False
    finally:
        return_connection(conn)


def record_donation(user_id, amount, charge_id):
    """تسجيل عملية دعم - الإصلاح 10"""
    if not user_id or amount <= 0:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO donations (user_id, amount, telegram_payment_charge_id)
            VALUES (%s, %s, %s)
        """, (user_id, amount, charge_id or ''))
        conn.commit()
        cur.close()
        
        clear_cache('donations_stats')
        clear_cache('stats')
        return True
    except Exception as e:
        print(f"record_donation error: {e}")
        return False
    finally:
        return_connection(conn)


def get_donations_stats():
    """إحصائيات الدعم مع cache - الإصلاح 11"""
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
        if result[0] > 0 or result[1] > 0:
            set_cache('donations_stats', result)
        return result
    except Exception as e:
        print(f"get_donations_stats error: {e}")
        return (0, 0)
    finally:
        return_connection(conn)


def record_suggestion(user_id, content, charge_id):
    """تسجيل اقتراح - الإصلاح 12"""
    if not user_id or not content:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO suggestions (user_id, content, telegram_payment_charge_id)
            VALUES (%s, %s, %s)
        """, (user_id, content[:500], charge_id or ''))
        conn.commit()
        cur.close()
        
        clear_cache('suggestions_count')
        clear_cache('stats')
        return True
    except Exception as e:
        print(f"record_suggestion error: {e}")
        return False
    finally:
        return_connection(conn)


def get_suggestions_count():
    """عدد الاقتراحات مع cache - الإصلاح 13"""
    cached = get_cache('suggestions_count')
    if cached is not None:
        return cached
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM suggestions")
        count = cur.fetchone()[0] or 0
        cur.close()
        
        if count > 0:
            set_cache('suggestions_count', count)
        return count
    except Exception as e:
        print(f"get_suggestions_count error: {e}")
        return 0
    finally:
        return_connection(conn)


def unregister_user(user_id):
    """إلغاء التسجيل - الإصلاح 14"""
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
        return removed
    except Exception as e:
        print(f"unregister_user error: {e}")
        return False
    finally:
        return_connection(conn)


def claim_daily_points(user_id):
    """مطالبة النقاط اليومية - الإصلاح 15"""
    if not user_id:
        return False, 0
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO user_points (user_id, total_points, last_claim_date)
            VALUES (%s, %s, CURRENT_DATE)
            ON CONFLICT (user_id) DO UPDATE
            SET total_points = user_points.total_points + %s,
                last_claim_date = CASE 
                    WHEN user_points.last_claim_date IS DISTINCT FROM CURRENT_DATE THEN CURRENT_DATE
                    ELSE user_points.last_claim_date
                END
            WHERE user_points.last_claim_date IS DISTINCT FROM CURRENT_DATE
            RETURNING total_points
        """, (user_id, DAILY_POINTS, DAILY_POINTS))
        
        row = cur.fetchone()
        if row:
            conn.commit()
            cur.close()
            points = row[0] or 0
            clear_cache(f'user_points_{user_id}')
            clear_cache('leaderboard')
            return True, points
        
        cur.execute("SELECT total_points FROM user_points WHERE user_id = %s", (user_id,))
        existing = cur.fetchone()
        conn.commit()
        cur.close()
        return False, (existing[0] if existing else 0)
    except Exception as e:
        print(f"claim_daily_points error: {e}")
        return False, 0
    finally:
        return_connection(conn)


def get_user_points(user_id):
    """الحصول على نقاط المستخدم - الإصلاح 16"""
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
        if points > 0:
            set_cache(key, points)
        return points
    except Exception as e:
        print(f"get_user_points error: {e}")
        return 0
    finally:
        return_connection(conn)


def get_leaderboard(limit=10):
    """لائحة الصدارة - الإصلاح 17"""
    cached = get_cache('leaderboard')
    if cached is not None:
        return cached
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT
                p.user_id,
                COALESCE(NULLIF(u.username, ''), NULLIF(u.first_name, ''), 'مستخدم') AS display_name,
                p.total_points
            FROM user_points p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.total_points > 0
            ORDER BY p.total_points DESC, p.user_id ASC
            LIMIT %s
        """, (min(limit, 20),))
        
        rows = cur.fetchall()
        cur.close()
        
        result = [{"user_id": r[0], "name": r[1] or 'مستخدم', "points": r[2] or 0} for r in rows]
        if result:
            set_cache('leaderboard', result)
        return result
    except Exception as e:
        print(f"get_leaderboard error: {e}")
        return []
    finally:
        return_connection(conn)


def mask_name(name):
    """تعتيم الاسم"""
    if not name:
        return "*"
    name = str(name).strip()[:20]
    if len(name) <= 1:
        return name
    return name[0] + "*" * (len(name) - 1)


def get_site_url():
    """الحصول على رابط الموقع"""
    manual = os.environ.get("APP_URL", "").strip()
    if manual:
        return manual.rstrip("/")
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    if domain:
        return f"https://{domain}"
    return ""


# ───────────────────────── تيليجرام ─────────────────────────

def send_message(chat_id, text, reply_markup=None):
    """إرسال رسالة - الإصلاح 18"""
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
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUEST_TIMEOUT)
        return True
    except Exception as e:
        print(f"send_message error: {e}")
        return False


def answer_callback(callback_id, text, show_alert=False):
    """الرد على زر"""
    try:
        requests.post(
            f"{TELEGRAM_API}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text[:200], "show_alert": show_alert},
            timeout=REQUEST_TIMEOUT
        )
    except Exception as e:
        print(f"answer_callback error: {e}")


def send_invoice(chat_id, amount, title, description, payload_str):
    """إرسال فاتورة - الإصلاح 19"""
    if not chat_id or amount <= 0:
        return False
    try:
        payload = {
            "chat_id": int(chat_id),
            "title": title[:32],
            "description": description[:255],
            "payload": payload_str[:128],
            "provider_token": os.environ.get("STRIPE_PROVIDER_TOKEN", ""),
            "currency": "XTR",
            "prices": [{"label": title[:32], "amount": int(amount)}],
            "start_parameter": payload_str[:64],
        }
        r = requests.post(f"{TELEGRAM_API}/sendInvoice", json=payload, timeout=REQUEST_TIMEOUT)
        result = r.json()
        success = result.get('ok', False)
        if not success:
            print(f"Invoice error: {result.get('description', 'unknown')}")
            send_message(chat_id, f"⚠️ خطأ: {result.get('description', 'حاول لاحقاً')[:100]}")
        return success
    except Exception as e:
        print(f"send_invoice exception: {e}")
        send_message(chat_id, f"❌ خطأ اتصال")
        return False


def answer_pre_checkout(pre_checkout_query_id, ok=True, error_message=None):
    """الرد على طلب ما قبل الدفع"""
    try:
        payload = {"pre_checkout_query_id": pre_checkout_query_id, "ok": ok}
        if error_message:
            payload["error_message"] = error_message[:200]
        requests.post(f"{TELEGRAM_API}/answerPreCheckoutQuery", json=payload, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        print(f"answer_pre_checkout error: {e}")


def donation_keyboard():
    """لوحة مفاتيح الدعم"""
    return {
        "inline_keyboard": [
            [{"text": f"⭐ {amount}", "callback_data": f"donate_{amount}"}]
            for amount in DONATION_AMOUNTS
        ]
    }


def main_keyboard():
    """اللوحة الرئيسية"""
    return {
        "inline_keyboard": [
            [{"text": "✅ بدي أتزوج", "callback_data": "want_marry"}],
            [
                {"text": "🎁 نقاط", "callback_data": "claim_points"},
                {"text": "🏆 صدارة", "callback_data": "show_leaderboard"},
            ],
            [{"text": "💻 موقع", "web_app": {"url": get_site_url() or "https://t.me"}}],
            [{"text": "❌ إلغاء", "callback_data": "unsubscribe"}],
        ]
    }


def build_leaderboard_text():
    """بناء نص لائحة الصدارة"""
    top = get_leaderboard(10)
    if not top:
        return "🙂 لا توجد بيانات"
    medals = ["🥇", "🥈", "🥉"]
    lines = ["🏆 <b>الصدارة:</b>\n"]
    for i, u in enumerate(top):
        medal = medals[i] if i < 3 else f"{i+1}."
        lines.append(f"{medal} {mask_name(u['name'])} — {u['points']}⭐")
    return "\n".join(lines)


def notify_admin_new_suggestion(user_id, username, content):
    """إشعار الأدمن"""
    if not ADMIN_CHAT_ID:
        return
    who = f"@{username}" if username else f"ID:{user_id}"
    send_message(ADMIN_CHAT_ID, f"💡 من {who}\n\n{content[:200]}")


# ───────────────────────── Gzip ─────────────────────────

@app.after_request
def gzip_response(response):
    """ضغط Gzip - الإصلاح 20"""
    if response.content_length is None or response.content_length < 500:
        return response
    
    accept = request.headers.get('Accept-Encoding', '')
    if 'gzip' in accept and 'gzip' not in response.headers.get('Content-Encoding', ''):
        try:
            buf = BytesIO()
            gz = gzip.GzipFile(mode='wb', fileobj=buf)
            gz.write(response.get_data())
            gz.close()
            response.set_data(buf.getvalue())
            response.headers['Content-Encoding'] = 'gzip'
        except:
            pass
    
    return response


# ───────────────────────── Webhook ─────────────────────────

@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    """معالج الويب هوك الرئيسي"""
    update = request.get_json(force=True, silent=True) or {}

    if "pre_checkout_query" in update:
        pcq = update["pre_checkout_query"]
        answer_pre_checkout(pcq["id"], ok=True)
        return jsonify({"ok": True})

    if "message" in update:
        msg = update["message"]
        chat_id = msg.get("chat", {}).get("id")
        text = (msg.get("text") or "").strip()

        sender = msg.get("from", {})
        user_id = sender.get("id")
        if user_id:
            upsert_user(user_id, username=sender.get("username"), first_name=sender.get("first_name"))

        if "successful_payment" in msg:
            sp = msg["successful_payment"]
            amount = sp.get("total_amount", 0)
            charge_id = sp.get("telegram_payment_charge_id", "")
            invoice_payload = sp.get("invoice_payload", "")

            if invoice_payload.startswith("suggest_"):
                send_message(chat_id, "✅ الآن اكتب اقتراحك:")
            else:
                record_donation(user_id, amount, charge_id)
                send_message(chat_id, f"💛 شكراً! تم استلام {amount}⭐")
            return jsonify({"ok": True})

        if text.startswith("/start"):
            send_message(chat_id, "👋 أهلاً في <b>بوت زوجوني</b> 💍", main_keyboard())
        elif text in ("/عدد", "/count"):
            send_message(chat_id, f"<b>المسجلون:</b> {get_count()} 💍")
        elif text in ("/نقاطي", "/points"):
            send_message(chat_id, f"<b>نقاطك:</b> {get_user_points(user_id)} ⭐")
        elif text in ("/الصدارة", "/leaderboard"):
            send_message(chat_id, build_leaderboard_text())
        elif text in ("/انسحب", "/unsubscribe"):
            if is_registered(user_id):
                send_message(chat_id, "متأكد؟", {
                    "inline_keyboard": [
                        [{"text": "✅ نعم", "callback_data": "confirm_unsubscribe"}],
                        [{"text": "🙅 لأ", "callback_data": "cancel_unsubscribe"}],
                    ]
                })
            else:
                send_message(chat_id, "أنت مش مسجل 🙂")

    elif "callback_query" in update:
        cq = update["callback_query"]
        sender = cq.get("from", {})
        user_id = sender.get("id")
        chat_id = cq.get("message", {}).get("chat", {}).get("id")
        callback_id = cq.get("id")
        data_key = cq.get("data", "")

        upsert_user(user_id, username=sender.get("username"), first_name=sender.get("first_name"))

        if data_key == "want_marry":
            if is_registered(user_id):
                answer_callback(callback_id, "أنت مسجل 😄")
            else:
                if register_user(user_id):
                    answer_callback(callback_id, "✅ تم!")
                    send_message(chat_id, f"🎉 تم تسجيلك!\n<b>الآن:</b> {get_count()} 💍")

        elif data_key == "claim_points":
            success, total = claim_daily_points(user_id)
            if success:
                answer_callback(callback_id, f"✅ +{DAILY_POINTS}!")
                send_message(chat_id, f"🎉 +{DAILY_POINTS}\n<b>المجموع:</b> {total}⭐")
            else:
                answer_callback(callback_id, "اليوم أخذت نقاطك", show_alert=True)

        elif data_key == "show_leaderboard":
            answer_callback(callback_id, "")
            send_message(chat_id, build_leaderboard_text())

        elif data_key == "unsubscribe":
            send_message(chat_id, "متأكد؟", {
                "inline_keyboard": [
                    [{"text": "✅ نعم", "callback_data": "confirm_unsubscribe"}],
                    [{"text": "🙅 لأ", "callback_data": "cancel_unsubscribe"}],
                ]
            })

        elif data_key == "confirm_unsubscribe":
            if unregister_user(user_id):
                answer_callback(callback_id, "✅ تم حذفك")
                send_message(chat_id, f"👋 حذفت\n<b>الآن:</b> {get_count()} 💍")

        elif data_key == "cancel_unsubscribe":
            answer_callback(callback_id, "👍 تم التراجع")
        
        elif data_key.startswith("donate_"):
            try:
                amount = int(data_key.split("_")[1])
                if amount in DONATION_AMOUNTS:
                    success = send_invoice(chat_id, amount, f"دعم {amount}⭐", "شكراً!", f"donate_{amount}")
                    answer_callback(callback_id, "✅" if success else "❌", show_alert=not success)
            except:
                answer_callback(callback_id, "❌ خطأ", show_alert=True)

    return jsonify({"ok": True})


# ───────────────────────── API ─────────────────────────

@app.route("/api/user/info", methods=["POST"])
def api_user_info():
    """معلومات المستخدم"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    
    if not user_id:
        return jsonify({"error": "missing"}), 400
    
    try:
        donations_count, _ = get_donations_stats()
        return jsonify({
            "user_id": user_id,
            "is_registered": is_registered(user_id),
            "points": get_user_points(user_id),
            "donation_count": donations_count,
        })
    except Exception as e:
        return jsonify({"error": str(e)[:50]}), 500


@app.route("/api/register", methods=["POST"])
def api_register():
    """تسجيل المستخدم"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    
    if not user_id:
        return jsonify({"error": "missing"}), 400
    
    try:
        result = register_user(user_id)
        return jsonify({
            "success": result,
            "message": "✅ تم!" if result else "مسجل مسبقاً"
        })
    except Exception as e:
        return jsonify({"error": str(e)[:50]}), 500


@app.route("/api/donate", methods=["POST"])
def api_donate():
    """معالجة الدفع"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    amount = data.get("amount")
    charge_id = data.get("charge_id", f"web_{int(time.time())}")
    
    if not user_id or not amount:
        return jsonify({"error": "missing"}), 400
    
    try:
        record_donation(user_id, amount, charge_id)
        return jsonify({
            "success": True,
            "message": f"💛 شكراً {amount}⭐"
        })
    except Exception as e:
        return jsonify({"error": str(e)[:50]}), 500


@app.route("/api/stats", methods=["GET"])
def api_stats():
    """الإحصائيات العامة"""
    try:
        cached = get_cache('stats')
        if cached is not None:
            return jsonify(cached)
        
        count = get_count()
        donations_count, donations_total = get_donations_stats()
        suggestions = get_suggestions_count()
        leaderboard = get_leaderboard(10)
        
        result = {
            "registered": count,
            "donations_count": donations_count,
            "donations_total": donations_total,
            "suggestions": suggestions,
            "leaderboard": leaderboard,
        }
        
        if count > 0 or donations_count > 0:
            set_cache('stats', result)
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)[:50]}), 500


@app.route("/api/claim-points", methods=["POST"])
def api_claim_points():
    """مطالبة النقاط"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    
    if not user_id:
        return jsonify({"error": "missing"}), 400
    
    try:
        success, total = claim_daily_points(user_id)
        return jsonify({
            "success": success,
            "points": total,
            "message": f"✅ +{DAILY_POINTS}!" if success else "اليوم أخذت نقاطك"
        })
    except Exception as e:
        return jsonify({"error": str(e)[:50]}), 500


@app.route("/api/send-invoice", methods=["POST"])
def api_send_invoice():
    """إرسال فاتورة من الموقع"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    amount = data.get("amount")
    
    if not user_id or not amount:
        return jsonify({"error": "missing"}), 400
    
    if amount not in DONATION_AMOUNTS:
        return jsonify({"error": "invalid_amount"}), 400
    
    try:
        success = send_invoice(user_id, amount, f"دعم {amount}⭐", "شكراً!", f"donate_{amount}")
        if success:
            return jsonify({
                "success": True,
                "message": f"✅ فاتورة {amount}⭐"
            })
        else:
            return jsonify({
                "success": False,
                "message": "❌ فشل"
            }), 500
    except Exception as e:
        print(f"api_send_invoice error: {e}")
        return jsonify({"error": str(e)[:50]}), 500


# ───────────────────────── الموقع ─────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>💍 بوت زوجوني</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1e1a2e 0%, #16213e 100%);
            color: #fff;
            min-height: 100vh;
            padding: 10px;
        }
        .container { max-width: 480px; margin: 0 auto; }
        .header { text-align: center; padding: 15px 0; margin-bottom: 15px; }
        .logo { font-size: 40px; }
        h1 { font-size: 22px; margin: 8px 0 0; background: linear-gradient(90deg, #ff5a8a, #7b3fe4); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .tabs { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-bottom: 15px; }
        .tab-btn { padding: 10px 8px; background: rgba(255,255,255,0.06); border: 2px solid rgba(255,255,255,0.1); color: #fff; border-radius: 8px; cursor: pointer; font-size: 12px; font-weight: 600; }
        .tab-btn.active { background: linear-gradient(90deg, #ff5a8a, #7b3fe4); border-color: #ff5a8a; }
        .tab-content { display: none; }
        .tab-content.active { display: block; animation: slideIn 0.2s ease; }
        @keyframes slideIn { from { opacity: 0; } to { opacity: 1; } }
        .card { background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; padding: 15px; margin-bottom: 12px; }
        .stat { display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,0.04); }
        .stat:last-child { border-bottom: 0; }
        .stat-label { font-size: 13px; color: #a0a0b0; }
        .stat-value { font-weight: 600; color: #ff5a8a; }
        .btn { width: 100%; padding: 12px; background: linear-gradient(90deg, #ff5a8a, #7b3fe4); color: #fff; border: 0; border-radius: 8px; font-weight: 600; cursor: pointer; margin-bottom: 8px; }
        .btn:active { opacity: 0.8; }
        .amount-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
        .amount-btn { padding: 12px; background: rgba(255,255,255,0.06); border: 2px solid rgba(255,255,255,0.1); color: #fff; border-radius: 8px; cursor: pointer; font-weight: 600; }
        .amount-btn:active { background: #ff5a8a; border-color: #ff5a8a; }
        .lb-item { display: flex; align-items: center; padding: 10px; background: rgba(255,255,255,0.02); border-radius: 8px; margin-bottom: 6px; font-size: 13px; }
        .lb-rank { font-size: 18px; margin-right: 10px; }
        .lb-info { flex: 1; }
        .lb-name { font-weight: 600; }
        .lb-points { color: #ff5a8a; font-size: 12px; }
        .loading { text-align: center; padding: 20px; color: #888; }
        .message { padding: 10px; border-radius: 6px; margin-bottom: 10px; text-align: center; font-size: 13px; animation: slideIn 0.2s; }
        .message.success { background: rgba(76, 175, 80, 0.15); color: #4caf50; }
        .message.error { background: rgba(244, 67, 54, 0.15); color: #f44336; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo">💍</div>
            <h1>زوجوني</h1>
        </div>

        <div class="tabs">
            <button class="tab-btn active" onclick="switchTab('home')">🏠</button>
            <button class="tab-btn" onclick="switchTab('leaderboard')">🏆</button>
            <button class="tab-btn" onclick="switchTab('donate')">⭐</button>
            <button class="tab-btn" onclick="switchTab('profile')">👤</button>
        </div>

        <div id="messages"></div>

        <div id="home" class="tab-content active">
            <div class="card" id="homeStats"><div class="loading">جاري...</div></div>
            <button class="btn" onclick="registerUser()">✅ تسجيل</button>
            <button class="btn" onclick="claimPoints()">🎁 نقاط اليوم</button>
        </div>

        <div id="leaderboard" class="tab-content">
            <div class="card" id="leaderboardList"><div class="loading">جاري...</div></div>
        </div>

        <div id="donate" class="tab-content">
            <div class="card">
                <div style="text-align: center; margin-bottom: 15px;">
                    <div style="font-size: 14px; margin-bottom: 10px;">⭐ اختر المبلغ</div>
                    <div class="amount-grid">
                        <button class="amount-btn" onclick="sendDonation(100)">100 ⭐</button>
                        <button class="amount-btn" onclick="sendDonation(200)">200 ⭐</button>
                        <button class="amount-btn" onclick="sendDonation(500)">500 ⭐</button>
                        <button class="amount-btn" onclick="sendDonation(1000)">1000 ⭐</button>
                    </div>
                </div>
            </div>
        </div>

        <div id="profile" class="tab-content">
            <div class="card" id="profileStats"><div class="loading">جاري...</div></div>
        </div>
    </div>

    <script>
        let tg = window.Telegram?.WebApp;
        let user = tg?.initDataUnsafe?.user || { id: 0 };
        let userId = user.id || 0;
        let statsCache = null;

        if (tg) tg.ready();

        function switchTab(tab) {
            document.querySelectorAll('.tab-content').forEach(e => e.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(e => e.classList.remove('active'));
            document.getElementById(tab).classList.add('active');
            event.target.classList.add('active');
            if (tab === 'leaderboard') loadLeaderboard();
            if (tab === 'profile') loadProfile();
        }

        function loadHome() {
            if (statsCache) { renderStats(statsCache); return; }
            fetch('/api/stats').then(r => r.json()).then(data => {
                if (data.registered !== undefined) {
                    statsCache = data;
                    renderStats(data);
                }
            }).catch(e => console.error(e));
        }

        function renderStats(d) {
            let html = `
                <div class="stat"><span class="stat-label">المسجلون</span><span class="stat-value">${d.registered || 0} 💍</span></div>
                <div class="stat"><span class="stat-label">الدعم</span><span class="stat-value">${d.donations_total || 0} ⭐</span></div>
                <div class="stat"><span class="stat-label">الاقتراحات</span><span class="stat-value">${d.suggestions || 0} 💡</span></div>
            `;
            document.getElementById('homeStats').innerHTML = html;
        }

        function loadLeaderboard() {
            if (statsCache?.leaderboard) { renderLeaderboard(statsCache.leaderboard); return; }
            fetch('/api/stats').then(r => r.json()).then(data => {
                if (data.leaderboard) {
                    statsCache = data;
                    renderLeaderboard(data.leaderboard);
                }
            }).catch(e => console.error(e));
        }

        function renderLeaderboard(lb) {
            let medals = ['🥇', '🥈', '🥉'];
            let html = lb.map((i, n) => `
                <div class="lb-item">
                    <span class="lb-rank">${medals[n] || n + 1}</span>
                    <div class="lb-info"><div class="lb-name">${i.name}</div><div class="lb-points">${i.points}⭐</div></div>
                </div>
            `).join('') || '<div class="loading">لا توجد بيانات</div>';
            document.getElementById('leaderboardList').innerHTML = html;
        }

        function loadProfile() {
            fetch('/api/user/info', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ user_id: userId }) })
                .then(r => r.json()).then(d => {
                    let html = `
                        <div class="stat"><span class="stat-label">ID</span><span class="stat-value">${d.user_id}</span></div>
                        <div class="stat"><span class="stat-label">الحالة</span><span class="stat-value">${d.is_registered ? '✅' : '❌'}</span></div>
                        <div class="stat"><span class="stat-label">النقاط</span><span class="stat-value">${d.points || 0} ⭐</span></div>
                    `;
                    document.getElementById('profileStats').innerHTML = html;
                }).catch(e => console.error(e));
        }

        function registerUser() {
            fetch('/api/register', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ user_id: userId }) })
                .then(r => r.json()).then(d => {
                    showMsg(d.message, d.success ? 'success' : 'error');
                    statsCache = null;
                }).catch(e => showMsg('خطأ', 'error'));
        }

        function claimPoints() {
            fetch('/api/claim-points', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ user_id: userId }) })
                .then(r => r.json()).then(d => {
                    showMsg(d.message, d.success ? 'success' : 'error');
                    statsCache = null;
                }).catch(e => showMsg('خطأ', 'error'));
        }

        function sendDonation(amount) {
            fetch('/api/send-invoice', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ user_id: userId, amount: amount }) })
                .then(r => r.json()).then(d => {
                    showMsg(d.message || d.error, d.success ? 'success' : 'error');
                    statsCache = null;
                }).catch(e => showMsg('خطأ اتصال', 'error'));
        }

        function showMsg(text, type) {
            let m = document.createElement('div');
            m.className = `message ${type}`;
            m.textContent = text;
            document.getElementById('messages').appendChild(m);
            setTimeout(() => m.remove(), 3000);
        }

        loadHome();
    </script>
</body>
</html>"""


@app.route("/")
def site_home():
    """الصفحة الرئيسية"""
    return make_response(HTML_TEMPLATE, 200, {'Content-Type': 'text/html; charset=utf-8'})


# ───────────────────────── تفعيل ─────────────────────────

def set_webhook():
    """تفعيل الويب هوك"""
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if not domain or not BOT_TOKEN:
        print("⚠️ بيانات الـ Webhook ناقصة")
        return
    url = f"https://{domain}/webhook/{BOT_TOKEN}"
    try:
        r = requests.get(
            f"{TELEGRAM_API}/setWebhook",
            params={
                "url": url,
                "allowed_updates": json.dumps(["message", "callback_query", "pre_checkout_query"]),
            },
            timeout=10,
        )
        print(f"✅ Webhook: {url}")
    except Exception as e:
        print(f"❌ Webhook error: {e}")


# تهيئة
init_db()
init_pool()
set_webhook()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
