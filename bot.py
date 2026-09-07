import os
import json
import requests
import psycopg2
import redis
import hmac
import hashlib
from psycopg2 import pool
from flask import Flask, request, jsonify, render_template_string, abort
from datetime import datetime, timedelta
from threading import Lock
from functools import wraps
import time
import gzip
from io import BytesIO

app = Flask(__name__)

# ─────────────────────────────────────────── CONFIG ───────────────────────────────────────────

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "super_secret_key")

DONATION_AMOUNTS = [100, 200, 500, 1000]
SUGGESTION_PRICE = 1
DAILY_POINTS = 100

# ─────────────────────────────────────────── DATABASE & REDIS ───────────────────────────────────────────

db_pool = None
pool_lock = Lock()
redis_client = None
redis_lock = Lock()

def init_redis():
    """تهيئة اتصال Redis"""
    global redis_client
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        print("✅ Redis متصل")
        return True
    except Exception as e:
        print(f"❌ خطأ Redis: {e}")
        return False


def init_pool():
    """تهيئة PostgreSQL Connection Pool"""
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
    """احصل على اتصال مع إعادة محاولة"""
    global db_pool
    if db_pool is None:
        if not init_pool():
            raise Exception("فشل إنشاء Pool")
    
    try:
        return db_pool.getconn()
    except Exception as e:
        if retry < 3:
            time.sleep(0.3)
            return get_connection(retry + 1)
        raise Exception(f"فشل الاتصال: {str(e)[:50]}")


def return_connection(conn, close=False):
    """أرجع الاتصال للـ pool"""
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


def redis_get(key):
    """احصل على قيمة من Redis"""
    try:
        if redis_client:
            return redis_client.get(key)
    except:
        pass
    return None


def redis_set(key, value, ttl=300):
    """احفظ قيمة في Redis"""
    try:
        if redis_client:
            redis_client.setex(key, ttl, value)
    except:
        pass


def redis_delete(key):
    """احذف قيمة من Redis"""
    try:
        if redis_client:
            redis_client.delete(key)
    except:
        pass


def redis_incr(key, ttl=60):
    """زيادة قيمة عداد"""
    try:
        if redis_client:
            redis_client.incr(key)
            redis_client.expire(key, ttl)
            return int(redis_client.get(key) or 0)
    except:
        pass
    return 0


# ─────────────────────────────────────────── RATE LIMITING ───────────────────────────────────────────

def rate_limit(max_calls=5, window=60):
    """ديكوريتر لـ Rate Limiting"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_id = request.json.get("user_id") if request.method == "POST" else request.args.get("user_id")
            if not user_id:
                user_id = request.remote_addr
            
            key = f"rate_limit:{f.__name__}:{user_id}"
            count = redis_incr(key, window)
            
            if count > max_calls:
                return jsonify({"error": "rate_limited"}), 429
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator


# ─────────────────────────────────────────── DATABASE OPERATIONS ───────────────────────────────────────

def init_db():
    """إنشاء الجداول والـ indexes"""
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
                charge_id TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS suggestions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                content TEXT,
                charge_id TEXT UNIQUE,
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
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                charge_id TEXT UNIQUE,
                type TEXT NOT NULL,
                amount INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_donations_user ON donations(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_donations_charge ON donations(charge_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_suggestions_user ON suggestions(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_points_total ON user_points(total_points DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_charge ON payments(charge_id)")

        conn.commit()
        cur.close()
        print("✅ قاعدة البيانات جاهزة")
    except Exception as e:
        print(f"❌ خطأ في DB: {e}")
    finally:
        return_connection(conn)


def upsert_user(user_id, username=None, first_name=None):
    """حفظ/تحديث المستخدم"""
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


def is_registered(user_id):
    """التحقق من التسجيل"""
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
    """تسجيل المستخدم"""
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
            redis_delete('count')
        return added
    except Exception as e:
        print(f"register_user error: {e}")
        return False
    finally:
        return_connection(conn)


def unregister_user(user_id):
    """إلغاء التسجيل"""
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
            redis_delete('count')
        return removed
    except Exception as e:
        print(f"unregister_user error: {e}")
        return False
    finally:
        return_connection(conn)


def get_count():
    """عدد المسجلين"""
    cached = redis_get('count')
    if cached:
        return int(cached)
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM registrations")
        count = cur.fetchone()[0] or 0
        cur.close()
        
        if count > 0:
            redis_set('count', str(count), 40)
        return count
    except Exception as e:
        print(f"get_count error: {e}")
        return 0
    finally:
        return_connection(conn)


def record_donation(user_id, amount, charge_id):
    """تسجيل دعم (منع الدفع المكرر)"""
    if not user_id or amount <= 0:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # التحقق من عدم وجود دفعة مكررة بنفس charge_id
        cur.execute("SELECT 1 FROM donations WHERE charge_id = %s", (charge_id,))
        if cur.fetchone():
            print(f"⚠️ دفعة مكررة: {charge_id}")
            cur.close()
            return False
        
        cur.execute("""
            INSERT INTO donations (user_id, amount, charge_id)
            VALUES (%s, %s, %s)
        """, (user_id, amount, charge_id))
        conn.commit()
        cur.close()
        
        redis_delete('donations_stats')
        redis_delete('stats')
        return True
    except Exception as e:
        print(f"record_donation error: {e}")
        return False
    finally:
        return_connection(conn)


def get_donations_stats():
    """إحصائيات الدعم"""
    cached = redis_get('donations_stats')
    if cached:
        data = json.loads(cached)
        return (data[0], data[1])
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM donations")
        row = cur.fetchone()
        cur.close()
        
        result = (row[0] or 0, row[1] or 0) if row else (0, 0)
        if result[0] > 0 or result[1] > 0:
            redis_set('donations_stats', json.dumps(result), 25)
        return result
    except Exception as e:
        print(f"get_donations_stats error: {e}")
        return (0, 0)
    finally:
        return_connection(conn)


def record_suggestion(user_id, content, charge_id):
    """تسجيل اقتراح مع منع الرسائل المكررة"""
    if not user_id or not content:
        return False
    
    # منع الرسائل المكررة بسرعة (كل 30 ثانية)
    last_suggestion_key = f"last_suggestion:{user_id}"
    if redis_get(last_suggestion_key):
        return False
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # التحقق من عدم وجود اقتراح مكرر بنفس charge_id
        if charge_id:
            cur.execute("SELECT 1 FROM suggestions WHERE charge_id = %s", (charge_id,))
            if cur.fetchone():
                print(f"⚠️ اقتراح مكرر: {charge_id}")
                cur.close()
                return False
        
        cur.execute("""
            INSERT INTO suggestions (user_id, content, charge_id)
            VALUES (%s, %s, %s)
        """, (user_id, content[:500], charge_id or None))
        conn.commit()
        cur.close()
        
        # تعيين توقيت لمنع الرسائل المكررة
        redis_set(last_suggestion_key, "1", 30)
        
        redis_delete('suggestions_count')
        redis_delete('stats')
        
        return True
    except Exception as e:
        print(f"record_suggestion error: {e}")
        return False
    finally:
        return_connection(conn)


def get_suggestions_count():
    """عدد الاقتراحات"""
    cached = redis_get('suggestions_count')
    if cached:
        return int(cached)
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM suggestions")
        count = cur.fetchone()[0] or 0
        cur.close()
        
        if count > 0:
            redis_set('suggestions_count', str(count), 40)
        return count
    except Exception as e:
        print(f"get_suggestions_count error: {e}")
        return 0
    finally:
        return_connection(conn)


def claim_daily_points(user_id):
    """مطالبة النقاط اليومية"""
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
            redis_delete(f'user_points_{user_id}')
            redis_delete('leaderboard')
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
    """نقاط المستخدم"""
    if not user_id:
        return 0
    key = f'user_points_{user_id}'
    cached = redis_get(key)
    if cached:
        return int(cached)
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT total_points FROM user_points WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
        
        points = row[0] if row else 0
        if points > 0:
            redis_set(key, str(points), 8)
        return points
    except Exception as e:
        print(f"get_user_points error: {e}")
        return 0
    finally:
        return_connection(conn)


def get_leaderboard(limit=10):
    """لائحة الصدارة"""
    cached = redis_get('leaderboard')
    if cached:
        return json.loads(cached)
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT p.user_id, COALESCE(NULLIF(u.username, ''), NULLIF(u.first_name, ''), 'مستخدم') AS display_name, p.total_points
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
            redis_set('leaderboard', json.dumps(result), 50)
        return result
    except Exception as e:
        print(f"get_leaderboard error: {e}")
        return []
    finally:
        return_connection(conn)


def get_all_suggestions(limit=50):
    """جلب جميع الاقتراحات (للأدمن)"""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT s.id, s.user_id, s.content, COALESCE(u.username, u.first_name, 'مستخدم') as name, s.created_at
            FROM suggestions s
            LEFT JOIN users u ON u.user_id = s.user_id
            ORDER BY s.created_at DESC
            LIMIT %s
        """, (limit,))
        
        rows = cur.fetchall()
        cur.close()
        return rows
    except Exception as e:
        print(f"get_all_suggestions error: {e}")
        return []
    finally:
        return_connection(conn)


# ─────────────────────────────────────────── TELEGRAM FUNCTIONS ───────────────────────────────────────────

def send_message(chat_id, text, reply_markup=None):
    """إرسال رسالة"""
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
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=8)
        return True
    except Exception as e:
        print(f"send_message error: {e}")
        return False


def answer_callback(callback_id, text, show_alert=False):
    """الرد على Callback"""
    try:
        requests.post(
            f"{TELEGRAM_API}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text[:200], "show_alert": show_alert},
            timeout=8
        )
    except:
        pass


def send_invoice(chat_id, amount, title, description, payload_str):
    """إرسال فاتورة Telegram Stars"""
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
        }
        
        r = requests.post(f"{TELEGRAM_API}/sendInvoice", json=payload, timeout=8)
        result = r.json()
        
        if result.get('ok'):
            print(f"✅ فاتورة أرسلت: {chat_id} - {amount}⭐")
            return True
        else:
            print(f"❌ خطأ الفاتورة: {result.get('description')}")
            send_message(chat_id, "⚠️ خطأ في الفاتورة، حاول لاحقاً")
            return False
    except Exception as e:
        print(f"❌ send_invoice: {e}")
        return False


def answer_pre_checkout(pre_checkout_query_id, ok=True, error_message=None):
    """الرد على Pre-Checkout"""
    try:
        payload = {"pre_checkout_query_id": pre_checkout_query_id, "ok": ok}
        if error_message:
            payload["error_message"] = error_message[:200]
        requests.post(f"{TELEGRAM_API}/answerPreCheckoutQuery", json=payload, timeout=8)
    except:
        pass


def main_keyboard():
    """لوحة الأزرار الرئيسية"""
    return {
        "inline_keyboard": [
            [{"text": "✅ بدي أتزوج", "callback_data": "want_marry"}],
            [
                {"text": "🎁 نقاطي", "callback_data": "claim_points"},
                {"text": "🏆 صدارة", "callback_data": "show_leaderboard"},
            ],
            [{"text": "💻 الموقع", "web_app": {"url": get_site_url() or "https://t.me"}}],
            [
                {"text": "💛 دعم", "callback_data": "show_donate"},
                {"text": "💬 اقتراح", "callback_data": "show_suggestion"},
            ],
            [{"text": "❌ إلغاء", "callback_data": "unsubscribe"}],
        ]
    }


def donation_keyboard():
    """لوحة التبرعات"""
    return {
        "inline_keyboard": [
            [{"text": f"⭐ {amount}", "callback_data": f"donate_{amount}"}]
            for amount in DONATION_AMOUNTS
        ]
    }


def leaderboard_text():
    """نص لائحة الصدارة"""
    top = get_leaderboard(10)
    if not top:
        return "🙂 لا توجد بيانات"
    medals = ["🥇", "🥈", "🥉"]
    lines = ["🏆 <b>الصدارة:</b>\n"]
    for i, u in enumerate(top):
        medal = medals[i] if i < 3 else f"{i+1}."
        name = str(u['name'])[:15]
        lines.append(f"{medal} {name} — {u['points']}⭐")
    return "\n".join(lines)


def get_site_url():
    """رابط الموقع"""
    manual = os.environ.get("APP_URL", "").strip()
    if manual:
        return manual.rstrip("/")
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    if domain:
        return f"https://{domain}"
    return ""


def verify_webhook_secret(request):
    """التحقق من سرية الـ Webhook"""
    secret = request.headers.get("X-Webhook-Secret", "")
    return secret == WEBHOOK_SECRET


# ─────────────────────────────────────────── GZIP COMPRESSION ───────────────────────────────────────────

@app.after_request
def gzip_response(response):
    """ضغط Gzip"""
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


# ─────────────────────────────────────────── WEBHOOK ───────────────────────────────────────────

@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    """معالج الويب هوك"""
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
                send_message(chat_id, "✅ شكراً! الآن اكتب اقتراحك:")
            elif amount > 0 and user_id:
                success = record_donation(user_id, amount, charge_id)
                if success:
                    send_message(chat_id, f"💛 شكراً لدعمك!\nاستقبلنا: <b>{amount}⭐</b>")
                    if ADMIN_CHAT_ID:
                        send_message(ADMIN_CHAT_ID, f"✅ دعم: {user_id} × {amount}⭐")
                else:
                    send_message(chat_id, "⚠️ خطأ في التسجيل")
            return jsonify({"ok": True})

        if text.startswith("/start"):
            send_message(chat_id, "👋 أهلاً في <b>بوت زوجوني</b> 💍", main_keyboard())
        elif text in ("/عدد", "/count"):
            send_message(chat_id, f"<b>المسجلون:</b> {get_count()} 💍")
        elif text in ("/نقاطي", "/points"):
            send_message(chat_id, f"<b>نقاطك:</b> {get_user_points(user_id)} ⭐")
        elif text in ("/الصدارة", "/leaderboard"):
            send_message(chat_id, leaderboard_text())
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
        data = cq.get("data", "")

        upsert_user(user_id, username=sender.get("username"), first_name=sender.get("first_name"))

        if data == "want_marry":
            if is_registered(user_id):
                answer_callback(callback_id, "أنت مسجل 😄")
            else:
                if register_user(user_id):
                    answer_callback(callback_id, "✅ تم!")
                    send_message(chat_id, f"🎉 تم تسجيلك!\n<b>المسجلون:</b> {get_count()} 💍")

        elif data == "claim_points":
            success, total = claim_daily_points(user_id)
            if success:
                answer_callback(callback_id, f"✅ +{DAILY_POINTS}!")
                send_message(chat_id, f"🎉 +{DAILY_POINTS}⭐\n<b>المجموع:</b> {total}⭐")
            else:
                answer_callback(callback_id, "اليوم أخذت نقاطك", show_alert=True)

        elif data == "show_leaderboard":
            answer_callback(callback_id, "")
            send_message(chat_id, leaderboard_text())

        elif data == "show_donate":
            answer_callback(callback_id, "")
            send_message(chat_id, "اختر مبلغ الدعم:", donation_keyboard())

        elif data == "show_suggestion":
            send_message(chat_id, "💬 اكتب اقتراحك (سيتم إرساله للأدمن)")

        elif data == "unsubscribe":
            send_message(chat_id, "متأكد؟", {
                "inline_keyboard": [
                    [{"text": "✅ نعم", "callback_data": "confirm_unsubscribe"}],
                    [{"text": "🙅 لأ", "callback_data": "cancel_unsubscribe"}],
                ]
            })

        elif data == "confirm_unsubscribe":
            if unregister_user(user_id):
                answer_callback(callback_id, "✅ تم حذفك")
                send_message(chat_id, f"👋 حذفت\n<b>الآن:</b> {get_count()} 💍")

        elif data == "cancel_unsubscribe":
            answer_callback(callback_id, "👍 تم التراجع")

        elif data.startswith("donate_"):
            try:
                amount = int(data.split("_")[1])
                if amount in DONATION_AMOUNTS:
                    success = send_invoice(chat_id, amount, f"دعم {amount}⭐", "شكراً!", f"donate_{amount}")
                    answer_callback(callback_id, "✅" if success else "❌", show_alert=not success)
            except:
                answer_callback(callback_id, "❌", show_alert=True)

    return jsonify({"ok": True})


# ─────────────────────────────────────────── API ENDPOINTS ───────────────────────────────────────────

@app.route("/api/user/info", methods=["POST"])
@rate_limit(max_calls=10, window=60)
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
@rate_limit(max_calls=5, window=60)
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


@app.route("/api/claim-points", methods=["POST"])
@rate_limit(max_calls=3, window=60)
def api_claim_points():
    """مطالبة النقاط اليومية"""
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


@app.route("/api/stats", methods=["GET"])
def api_stats():
    """الإحصائيات"""
    try:
        cached = redis_get('stats')
        if cached:
            return jsonify(json.loads(cached))
        
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
            redis_set('stats', json.dumps(result), 25)
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)[:50]}), 500


@app.route("/api/send-invoice", methods=["POST"])
@rate_limit(max_calls=5, window=60)
def api_send_invoice():
    """إرسال فاتورة من الويب"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    amount = data.get("amount")
    
    if not user_id or not amount:
        return jsonify({"error": "missing"}), 400
    
    if amount not in DONATION_AMOUNTS:
        return jsonify({"error": "invalid_amount"}), 400
    
    try:
        success = send_invoice(user_id, amount, f"دعم {amount}⭐", "شكراً!", f"donate_{amount}")
        return jsonify({
            "success": success,
            "message": f"✅ فاتورة {amount}⭐" if success else "❌ فشل"
        })
    except Exception as e:
        return jsonify({"error": str(e)[:50]}), 500


# ─────────────────────────────────────────── ADMIN PANEL ───────────────────────────────────────────

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")

@app.route("/admin")
def admin_login():
    """صفحة دخول الأدمن"""
    return render_template_string("""
    <!DOCTYPE html>
    <html dir="rtl" lang="ar">
    <head>
        <meta charset="UTF-8">
        <title>لوحة الأدمن</title>
        <style>
            * { box-sizing: border-box; }
            body { font-family: Arial, sans-serif; background: #1a1a1a; color: #fff; margin: 0; padding: 20px; }
            .container { max-width: 500px; margin: 50px auto; }
            .login-box { background: #2d2d2d; padding: 30px; border-radius: 10px; text-align: center; }
            h1 { margin-top: 0; }
            input { width: 100%; padding: 10px; margin: 10px 0; border: none; border-radius: 5px; font-size: 16px; }
            button { width: 100%; padding: 10px; background: #ff5a8a; color: #fff; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; }
            button:hover { background: #d9427f; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="login-box">
                <h1>🔐 لوحة الأدمن</h1>
                <form method="post" action="/admin/login">
                    <input type="password" name="password" placeholder="كلمة المرور" required>
                    <button type="submit">دخول</button>
                </form>
            </div>
        </div>
    </body>
    </html>
    """)


@app.route("/admin/login", methods=["POST"])
def admin_login_check():
    """التحقق من كلمة المرور"""
    password = request.form.get("password", "")
    if password == ADMIN_PASSWORD:
        return redirect("/admin/dashboard")
    return redirect("/admin")


@app.route("/admin/dashboard")
def admin_dashboard():
    """لوحة التحكم"""
    # ملاحظة: في الإنتاج أضف جلسات/tokens أفضل
    suggestions = get_all_suggestions(100)
    donations_count, donations_total = get_donations_stats()
    count = get_count()
    
    suggestions_html = "".join([
        f'<div style="background: #333; padding: 15px; margin: 10px 0; border-radius: 8px;">'
        f'<p><b>#{r[0]}</b> من <b>@{r[3]}</b> (ID: {r[1]})</p>'
        f'<p>{r[2]}</p>'
        f'<small>{r[4]}</small>'
        f'</div>'
        for r in suggestions
    ])
    
    return render_template_string("""
    <!DOCTYPE html>
    <html dir="rtl" lang="ar">
    <head>
        <meta charset="UTF-8">
        <title>لوحة الأدمن</title>
        <style>
            * { box-sizing: border-box; }
            body { font-family: Arial, sans-serif; background: #1a1a1a; color: #fff; margin: 0; padding: 20px; }
            .container { max-width: 900px; margin: 0 auto; }
            h1 { text-align: center; }
            .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-bottom: 30px; }
            .stat-box { background: #2d2d2d; padding: 20px; border-radius: 10px; text-align: center; }
            .stat-box h2 { margin: 0; font-size: 24px; color: #ff5a8a; }
            .stat-box p { margin: 5px 0 0; font-size: 12px; color: #aaa; }
            .suggestions { background: #2d2d2d; padding: 20px; border-radius: 10px; }
            .suggestions h2 { margin-top: 0; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 لوحة الأدمن</h1>
            
            <div class="stats">
                <div class="stat-box">
                    <h2>{{ registered }}</h2>
                    <p>مسجلون 💍</p>
                </div>
                <div class="stat-box">
                    <h2>{{ donations_count }}</h2>
                    <p>تبرعات 💛</p>
                </div>
                <div class="stat-box">
                    <h2>{{ donations_total }}⭐</h2>
                    <p>الإجمالي</p>
                </div>
            </div>
            
            <div class="suggestions">
                <h2>💬 الاقتراحات</h2>
                {{ suggestions_html | safe }}
            </div>
        </div>
    </body>
    </html>
    """, registered=count, donations_count=donations_count, donations_total=donations_total, suggestions_html=suggestions_html)


# ─────────────────────────────────────────── WEB APP ───────────────────────────────────────────

WEBAPP_HTML = """<!DOCTYPE html>
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
        .tab-btn { padding: 10px 8px; background: rgba(255,255,255,0.06); border: 2px solid rgba(255,255,255,0.1); color: #fff; border-radius: 8px; cursor: pointer; font-size: 12px; font-weight: 600; transition: all 0.2s; }
        .tab-btn.active { background: linear-gradient(90deg, #ff5a8a, #7b3fe4); border-color: #ff5a8a; }
        .tab-btn:hover { transform: translateY(-2px); }
        
        .tab-content { display: none; }
        .tab-content.active { display: block; animation: slideIn 0.2s ease; }
        @keyframes slideIn { from { opacity: 0; } to { opacity: 1; } }
        
        .button { width: 100%; padding: 12px; margin: 8px 0; background: linear-gradient(90deg, #ff5a8a, #7b3fe4); color: #fff; border: none; border-radius: 8px; cursor: pointer; font-weight: 600; }
        .button:hover { opacity: 0.9; }
        .button:disabled { opacity: 0.5; cursor: not-allowed; }
        
        .stat-card { background: rgba(255,255,255,0.06); padding: 15px; border-radius: 8px; margin: 10px 0; border-left: 4px solid #ff5a8a; }
        .stat-card h3 { margin: 0 0 5px; font-size: 14px; }
        .stat-card p { margin: 0; font-size: 22px; font-weight: 600; }
        
        .leaderboard { background: rgba(255,255,255,0.06); border-radius: 8px; }
        .leaderboard-item { padding: 10px 15px; border-bottom: 1px solid rgba(255,255,255,0.1); display: flex; justify-content: space-between; align-items: center; }
        .leaderboard-item:last-child { border-bottom: none; }
        .medal { font-size: 18px; margin-right: 10px; }
        
        .donation-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
        .donation-btn { padding: 15px; background: rgba(255,255,255,0.06); border: 2px solid rgba(255,255,255,0.1); color: #fff; border-radius: 8px; cursor: pointer; font-weight: 600; }
        .donation-btn:hover { background: rgba(255,255,255,0.1); }
        
        .input-field { width: 100%; padding: 10px; margin: 8px 0; background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.1); color: #fff; border-radius: 8px; }
        .input-field::placeholder { color: rgba(255,255,255,0.4); }
        
        .loading { text-align: center; padding: 20px; }
        .spinner { display: inline-block; width: 30px; height: 30px; border: 3px solid rgba(255,255,255,0.2); border-top-color: #ff5a8a; border-radius: 50%; animation: spin 0.6s linear infinite; }
        @keyframes spin { to { transform: rotate(360deg); } }
        
        .status { padding: 10px; border-radius: 8px; margin: 10px 0; text-align: center; font-weight: 600; }
        .status.success { background: rgba(76, 175, 80, 0.2); color: #4caf50; }
        .status.error { background: rgba(244, 67, 54, 0.2); color: #f44336; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo">💍</div>
            <h1>بوت زوجوني</h1>
        </div>
        
        <div class="tabs">
            <button class="tab-btn active" onclick="switchTab('home')">الرئيسية</button>
            <button class="tab-btn" onclick="switchTab('leaderboard')">صدارة</button>
            <button class="tab-btn" onclick="switchTab('donate')">دعم</button>
            <button class="tab-btn" onclick="switchTab('suggestion')">اقتراح</button>
        </div>

        <!-- HOME TAB -->
        <div id="home" class="tab-content active">
            <div id="user-info">
                <div class="loading"><div class="spinner"></div></div>
            </div>
        </div>

        <!-- LEADERBOARD TAB -->
        <div id="leaderboard" class="tab-content">
            <div id="leaderboard-content">
                <div class="loading"><div class="spinner"></div></div>
            </div>
        </div>

        <!-- DONATE TAB -->
        <div id="donate" class="tab-content">
            <p style="text-align:center; font-size:14px;">اختر المبلغ الذي تريد دعمه:</p>
            <div class="donation-grid" id="donation-buttons"></div>
        </div>

        <!-- SUGGESTION TAB -->
        <div id="suggestion" class="tab-content">
            <p style="text-align:center; font-size:14px;">شارك اقتراحك معنا:</p>
            <textarea class="input-field" id="suggestion-text" placeholder="اكتب اقتراحك..." style="height: 100px; resize: none;"></textarea>
            <button class="button" onclick="sendSuggestion()">📤 إرسال</button>
        </div>
    </div>

    <script>
        let tg = window.Telegram.WebApp;
        let userId = tg.initDataUnsafe?.user?.id || "unknown";
        
        tg.expand();
        tg.headerColor = "#1e1a2e";

        async function apiCall(endpoint, data = {}) {
            try {
                const res = await fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, ...data })
                });
                return await res.json();
            } catch (e) {
                console.error(e);
                return null;
            }
        }

        async function loadUserInfo() {
            const data = await apiCall('/api/user/info');
            if (!data) return;
            
            const html = `
                <div class="stat-card">
                    <h3>🎁 نقاطك</h3>
                    <p>${data.points} ⭐</p>
                </div>
                <div class="stat-card">
                    <h3>👥 الحالة</h3>
                    <p>${data.is_registered ? '✅ مسجل' : '❌ غير مسجل'}</p>
                </div>
                <button class="button" onclick="registerOrClaimPoints()">${data.is_registered ? '🎁 مطالبة النقاط' : '✅ التسجيل'}</button>
            `;
            document.getElementById('user-info').innerHTML = html;
        }

        async function registerOrClaimPoints() {
            const info = await apiCall('/api/user/info');
            if (info?.is_registered) {
                const res = await apiCall('/api/claim-points');
                showStatus(res?.message || 'تم', res?.success ? 'success' : 'error');
            } else {
                const res = await apiCall('/api/register');
                showStatus(res?.message || 'تم', res?.success ? 'success' : 'error');
            }
            loadUserInfo();
        }

        async function loadLeaderboard() {
            const data = await apiCall('/api/stats');
            if (!data?.leaderboard) return;
            
            const medals = ['🥇', '🥈', '🥉'];
            const html = `
                <div class="leaderboard">
                    ${data.leaderboard.map((u, i) => `
                        <div class="leaderboard-item">
                            <div>
                                <span class="medal">${medals[i] || `${i+1}.`}</span>
                                <b>${u.name}</b>
                            </div>
                            <div><b>${u.points}⭐</b></div>
                        </div>
                    `).join('')}
                </div>
            `;
            document.getElementById('leaderboard-content').innerHTML = html;
        }

        function loadDonationButtons() {
            const amounts = [100, 200, 500, 1000];
            const html = amounts.map(a => `
                <button class="donation-btn" onclick="sendDonationInvoice(${a})">
                    ⭐ ${a}
                </button>
            `).join('');
            document.getElementById('donation-buttons').innerHTML = html;
        }

        async function sendDonationInvoice(amount) {
            const res = await apiCall('/api/send-invoice', { amount });
            if (res?.success) {
                showStatus('تم إرسال الفاتورة للبوت', 'success');
            } else {
                showStatus('خطأ في الإرسال', 'error');
            }
        }

        async function sendSuggestion() {
            const text = document.getElementById('suggestion-text').value.trim();
            if (!text) {
                showStatus('اكتب اقتراحك أولاً', 'error');
                return;
            }
            
            const res = await apiCall('/api/send-suggestion', { content: text });
            if (res?.success) {
                showStatus('تم إرسال الاقتراح', 'success');
                document.getElementById('suggestion-text').value = '';
            } else {
                showStatus('خطأ أو رسالة مكررة', 'error');
            }
        }

        function switchTab(tab) {
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
            document.getElementById(tab).classList.add('active');
            event.target.classList.add('active');
            
            if (tab === 'leaderboard') loadLeaderboard();
            if (tab === 'donate') loadDonationButtons();
        }

        function showStatus(msg, type) {
            const el = document.createElement('div');
            el.className = `status ${type}`;
            el.textContent = msg;
            document.body.appendChild(el);
            setTimeout(() => el.remove(), 3000);
        }

        loadUserInfo();
    </script>
</body>
</html>
"""

@app.route("/")
def index():
    """الصفحة الرئيسية للويب أب"""
    return WEBAPP_HTML


# ─────────────────────────────────────────── INITIALIZATION & RUN ───────────────────────────────────────────

if __name__ == "__main__":
    from flask import redirect
    init_redis()
    init_db()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
