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
import hashlib
import hmac

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")
STRIPE_TOKEN = os.environ.get("STRIPE_PROVIDER_TOKEN", "")

DONATION_AMOUNTS = [100, 200, 500, 1000]
DAILY_POINTS = 100

db_pool = None
pool_lock = Lock()
cache = {}
cache_lock = Lock()

CACHE_TTL = {'stats': 25, 'leaderboard': 50, 'user_points': 8, 'count': 40}

MAX_RETRIES = 3
RETRY_DELAY = 0.3
REQUEST_TIMEOUT = 8


def init_pool():
    global db_pool
    if not DATABASE_URL:
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
        return True
    except Exception as e:
        print(f"Pool init error: {e}")
        return False


def get_connection(retry=0):
    global db_pool
    if db_pool is None:
        if not init_pool():
            raise Exception("DB Pool failed")
    try:
        return db_pool.getconn()
    except (pool.PoolError, psycopg2.OperationalError) as e:
        if retry < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return get_connection(retry + 1)
        raise


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
            ttl = CACHE_TTL.get(key, 60)
            if data is not None and datetime.now() - timestamp < timedelta(seconds=ttl):
                return data
            try:
                del cache[key]
            except:
                pass
    return None


def set_cache(key, data):
    if data is None or (isinstance(data, (int, list)) and not data):
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
                status TEXT DEFAULT 'success',
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

        cur.execute("CREATE INDEX IF NOT EXISTS idx_donations_user ON donations(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_donations_charge ON donations(charge_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_points_total ON user_points(total_points DESC)")

        conn.commit()
        cur.close()
    except Exception as e:
        print(f"DB init error: {e}")
    finally:
        return_connection(conn)


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
        print(f"upsert_user error: {e}")
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
        if count > 0:
            set_cache('count', count)
        return count
    except Exception as e:
        print(f"get_count error: {e}")
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
        print(f"is_registered error: {e}")
        return False
    finally:
        return_connection(conn)


def register_user(user_id):
    if not user_id:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO registrations (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (user_id,))
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
    if not user_id or amount <= 0 or not charge_id:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO donations (user_id, amount, charge_id, status)
            VALUES (%s, %s, %s, 'success')
            ON CONFLICT (charge_id) DO NOTHING
        """, (user_id, amount, charge_id))
        
        if cur.rowcount == 0:
            cur.close()
            return_connection(conn)
            return False
            
        conn.commit()
        cur.close()
        clear_cache('stats')
        return True
    except Exception as e:
        print(f"record_donation error: {e}")
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
        cur.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM donations WHERE status='success'")
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


def claim_daily_points(user_id):
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
    cached = get_cache('leaderboard')
    if cached is not None:
        return cached
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT p.user_id,
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
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=REQUEST_TIMEOUT)
        return True
    except Exception as e:
        print(f"send_message error: {e}")
        return False


@app.after_request
def gzip_response(response):
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


@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    update = request.get_json(force=True, silent=True) or {}

    if "pre_checkout_query" in update:
        pcq = update["pre_checkout_query"]
        try:
            requests.post(
                f"{TELEGRAM_API}/answerPreCheckoutQuery",
                json={"pre_checkout_query_id": pcq["id"], "ok": True},
                timeout=REQUEST_TIMEOUT
            )
        except:
            pass
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
            
            if amount > 0 and user_id and charge_id:
                success = record_donation(user_id, amount, charge_id)
                if success:
                    send_message(chat_id, f"💛 شكراً!\nتم استلام: <b>{amount}⭐</b>")
            return jsonify({"ok": True})

        if text.startswith("/start"):
            send_message(chat_id, "👋 أهلاً بك 💍")

    return jsonify({"ok": True})


@app.route("/api/user/info", methods=["POST"])
def api_user_info():
    try:
        data = request.get_json() or {}
        user_id = data.get("user_id")
        
        if not user_id:
            return jsonify({"error": "no_user_id"}), 400
        
        try:
            user_id = int(user_id)
        except:
            return jsonify({"error": "invalid_user_id"}), 400
        
        donations_count, _ = get_donations_stats()
        return jsonify({
            "user_id": user_id,
            "is_registered": is_registered(user_id),
            "points": get_user_points(user_id),
            "donation_count": donations_count,
            "status": "ok"
        })
    except Exception as e:
        print(f"api_user_info error: {e}")
        return jsonify({"error": str(e)[:30], "status": "error"}), 500


@app.route("/api/register", methods=["POST"])
def api_register():
    try:
        data = request.get_json() or {}
        user_id = data.get("user_id")
        
        if not user_id:
            return jsonify({"error": "no_user_id"}), 400
        
        try:
            user_id = int(user_id)
        except:
            return jsonify({"error": "invalid_user_id"}), 400
        
        result = register_user(user_id)
        return jsonify({
            "success": result,
            "message": "✅ تم!" if result else "مسجل مسبقاً",
            "count": get_count()
        })
    except Exception as e:
        print(f"api_register error: {e}")
        return jsonify({"error": str(e)[:30], "success": False}), 500


@app.route("/api/stats", methods=["GET"])
def api_stats():
    try:
        cached = get_cache('stats')
        if cached is not None:
            return jsonify(cached)
        
        count = get_count()
        donations_count, donations_total = get_donations_stats()
        leaderboard = get_leaderboard(10)
        
        result = {
            "registered": max(0, count),
            "donations_count": max(0, donations_count),
            "donations_total": max(0, donations_total),
            "leaderboard": leaderboard or [],
            "status": "ok"
        }
        
        if count > 0 or donations_count > 0:
            set_cache('stats', result)
        
        return jsonify(result)
    except Exception as e:
        print(f"api_stats error: {e}")
        return jsonify({
            "registered": 0,
            "donations_count": 0,
            "donations_total": 0,
            "leaderboard": [],
            "error": str(e)[:30],
            "status": "error"
        }), 500


@app.route("/api/claim-points", methods=["POST"])
def api_claim_points():
    try:
        data = request.get_json() or {}
        user_id = data.get("user_id")
        
        if not user_id:
            return jsonify({"error": "no_user_id"}), 400
        
        try:
            user_id = int(user_id)
        except:
            return jsonify({"error": "invalid_user_id"}), 400
        
        success, total = claim_daily_points(user_id)
        return jsonify({
            "success": success,
            "points": max(0, total),
            "message": f"✅ +{DAILY_POINTS}!" if success else "اليوم أخذت نقاطك"
        })
    except Exception as e:
        print(f"api_claim_points error: {e}")
        return jsonify({"error": str(e)[:30], "success": False}), 500


@app.route("/api/create-payment", methods=["POST"])
def api_create_payment():
    try:
        data = request.get_json() or {}
        user_id = data.get("user_id")
        amount = data.get("amount")
        
        if not user_id or not amount:
            return jsonify({"error": "missing_params"}), 400
        
        try:
            user_id = int(user_id)
            amount = int(amount)
        except:
            return jsonify({"error": "invalid_params"}), 400
        
        if amount not in DONATION_AMOUNTS:
            return jsonify({"error": "invalid_amount"}), 400
        
        if not STRIPE_TOKEN:
            return jsonify({"error": "payment_unavailable"}), 503
        
        charge_id = f"web_{user_id}_{int(time.time())}_{amount}"
        
        return jsonify({
            "success": True,
            "charge_id": charge_id,
            "amount": amount,
            "user_id": user_id,
            "currency": "XTR",
            "title": f"دعم {amount}⭐",
            "description": "شكراً لدعمك! 💛"
        })
    except Exception as e:
        print(f"api_create_payment error: {e}")
        return jsonify({"error": str(e)[:30], "success": False}), 500


@app.route("/api/complete-payment", methods=["POST"])
def api_complete_payment():
    try:
        data = request.get_json() or {}
        user_id = data.get("user_id")
        amount = data.get("amount")
        charge_id = data.get("charge_id")
        
        if not user_id or not amount or not charge_id:
            return jsonify({"error": "missing_params"}), 400
        
        try:
            user_id = int(user_id)
            amount = int(amount)
        except:
            return jsonify({"error": "invalid_params"}), 400
        
        if amount not in DONATION_AMOUNTS:
            return jsonify({"error": "invalid_amount"}), 400
        
        if not charge_id or not str(charge_id).startswith("web_"):
            return jsonify({"error": "invalid_charge_id"}), 400
        
        success = record_donation(user_id, amount, charge_id)
        
        if success:
            send_message(ADMIN_CHAT_ID, f"✅ دعم: {user_id} × {amount}⭐") if ADMIN_CHAT_ID else None
            return jsonify({
                "success": True,
                "message": f"💛 شكراً! {amount}⭐",
                "points": get_user_points(user_id)
            })
        else:
            return jsonify({
                "error": "payment_duplicate",
                "message": "هذه العملية مسجلة مسبقاً"
            }), 400
    except Exception as e:
        print(f"api_complete_payment error: {e}")
        return jsonify({"error": str(e)[:30], "success": False}), 500


@app.route("/")
def site_home():
    html = """<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>💍 زوجوني</title>
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
        .logo { font-size: 40px; margin: 0; }
        h1 { font-size: 22px; margin: 8px 0 0; background: linear-gradient(90deg, #ff5a8a, #7b3fe4); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .tabs { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-bottom: 15px; }
        .tab-btn { padding: 10px 8px; background: rgba(255,255,255,0.06); border: 2px solid rgba(255,255,255,0.1); color: #fff; border-radius: 8px; cursor: pointer; font-size: 12px; font-weight: 600; transition: 0.2s; }
        .tab-btn.active { background: linear-gradient(90deg, #ff5a8a, #7b3fe4); border-color: #ff5a8a; }
        .tab-content { display: none; }
        .tab-content.active { display: block; animation: slideIn 0.2s ease; }
        @keyframes slideIn { from { opacity: 0; } to { opacity: 1; } }
        .card { background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; padding: 15px; margin-bottom: 12px; }
        .stat { display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,0.04); }
        .stat:last-child { border-bottom: 0; }
        .stat-label { font-size: 13px; color: #a0a0b0; }
        .stat-value { font-weight: 600; color: #ff5a8a; }
        .btn { width: 100%; padding: 12px; background: linear-gradient(90deg, #ff5a8a, #7b3fe4); color: #fff; border: 0; border-radius: 8px; font-weight: 600; cursor: pointer; margin-bottom: 8px; transition: 0.2s; }
        .btn:active { opacity: 0.8; transform: scale(0.98); }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .amount-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
        .amount-btn { padding: 12px; background: rgba(255,255,255,0.06); border: 2px solid rgba(255,255,255,0.1); color: #fff; border-radius: 8px; cursor: pointer; font-weight: 600; transition: 0.2s; }
        .amount-btn:active { background: #ff5a8a; border-color: #ff5a8a; transform: scale(0.95); }
        .amount-btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .lb-item { display: flex; align-items: center; padding: 10px; background: rgba(255,255,255,0.02); border-radius: 8px; margin-bottom: 6px; font-size: 13px; }
        .lb-rank { font-size: 18px; margin-right: 10px; min-width: 20px; }
        .lb-info { flex: 1; }
        .lb-name { font-weight: 600; }
        .lb-points { color: #ff5a8a; font-size: 12px; }
        .loading { text-align: center; padding: 20px; color: #888; }
        .message { padding: 10px; border-radius: 6px; margin-bottom: 10px; text-align: center; font-size: 13px; animation: slideIn 0.2s; }
        .message.success { background: rgba(76, 175, 80, 0.15); color: #4caf50; }
        .message.error { background: rgba(244, 67, 54, 0.15); color: #f44336; }
        .message.info { background: rgba(33, 150, 243, 0.15); color: #2196f3; }
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
            <div class="card" id="homeStats"><div class="loading">جاري التحميل...</div></div>
            <button class="btn" onclick="registerUser()">✅ تسجيل</button>
            <button class="btn" onclick="claimPoints()">🎁 نقاط اليوم</button>
        </div>

        <div id="leaderboard" class="tab-content">
            <div class="card" id="leaderboardList"><div class="loading">جاري التحميل...</div></div>
        </div>

        <div id="donate" class="tab-content">
            <div class="card">
                <div style="text-align: center; margin-bottom: 15px;">
                    <div style="font-size: 14px; margin-bottom: 10px;">⭐ اختر المبلغ</div>
                    <div class="amount-grid">
                        <button class="amount-btn" onclick="startPayment(100)" id="btn100">💛 100</button>
                        <button class="amount-btn" onclick="startPayment(200)" id="btn200">💛 200</button>
                        <button class="amount-btn" onclick="startPayment(500)" id="btn500">💛 500</button>
                        <button class="amount-btn" onclick="startPayment(1000)" id="btn1000">💛 1000</button>
                    </div>
                </div>
            </div>
        </div>

        <div id="profile" class="tab-content">
            <div class="card" id="profileStats"><div class="loading">جاري التحميل...</div></div>
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
            if (tab === 'home') loadHome();
        }

        function apiCall(endpoint, method = 'GET', body = null) {
            return new Promise((resolve, reject) => {
                let opts = { timeout: 8000 };
                if (method !== 'GET') {
                    opts.method = method;
                    opts.headers = { 'Content-Type': 'application/json' };
                    opts.body = JSON.stringify(body);
                }
                
                fetch(endpoint, opts)
                    .then(r => {
                        if (!r.ok) throw new Error(`HTTP ${r.status}`);
                        return r.json();
                    })
                    .then(data => {
                        if (data.error) throw new Error(data.error);
                        resolve(data);
                    })
                    .catch(e => reject(e));
            });
        }

        function loadHome() {
            if (statsCache) { renderStats(statsCache); return; }
            apiCall('/api/stats').then(data => {
                if (data.registered !== undefined) {
                    statsCache = data;
                    renderStats(data);
                }
            }).catch(e => showMsg('خطأ في تحميل البيانات', 'error'));
        }

        function renderStats(d) {
            let html = `
                <div class="stat"><span class="stat-label">المسجلون</span><span class="stat-value">${d.registered || 0} 💍</span></div>
                <div class="stat"><span class="stat-label">الدعم</span><span class="stat-value">${d.donations_total || 0} ⭐</span></div>
            `;
            document.getElementById('homeStats').innerHTML = html;
        }

        function loadLeaderboard() {
            if (statsCache?.leaderboard) { renderLeaderboard(statsCache.leaderboard); return; }
            apiCall('/api/stats').then(data => {
                if (data.leaderboard) {
                    statsCache = data;
                    renderLeaderboard(data.leaderboard);
                }
            }).catch(e => showMsg('خطأ في تحميل الصدارة', 'error'));
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
            apiCall('/api/user/info', 'POST', { user_id: userId }).then(d => {
                let html = `
                    <div class="stat"><span class="stat-label">ID</span><span class="stat-value">${d.user_id}</span></div>
                    <div class="stat"><span class="stat-label">الحالة</span><span class="stat-value">${d.is_registered ? '✅' : '❌'}</span></div>
                    <div class="stat"><span class="stat-label">النقاط</span><span class="stat-value">${d.points || 0} ⭐</span></div>
                `;
                document.getElementById('profileStats').innerHTML = html;
            }).catch(e => showMsg('خطأ في تحميل الملف', 'error'));
        }

        function registerUser() {
            apiCall('/api/register', 'POST', { user_id: userId }).then(d => {
                showMsg(d.message, d.success ? 'success' : 'error');
                statsCache = null;
                loadHome();
            }).catch(e => showMsg('خطأ: ' + e.message, 'error'));
        }

        function claimPoints() {
            apiCall('/api/claim-points', 'POST', { user_id: userId }).then(d => {
                showMsg(d.message, d.success ? 'success' : 'error');
                statsCache = null;
                if (d.success) loadProfile();
            }).catch(e => showMsg('خطأ: ' + e.message, 'error'));
        }

        async function startPayment(amount) {
            if (!userId) {
                showMsg('خطأ: لم يتم تحميل بيانات المستخدم', 'error');
                return;
            }

            let btn = document.getElementById(`btn${amount}`);
            btn.disabled = true;
            btn.textContent = '⏳...';

            try {
                showMsg('⏳ جاري إعداد الدفع...', 'info');
                
                let createResp = await apiCall('/api/create-payment', 'POST', { 
                    user_id: userId, 
                    amount: amount 
                });

                if (!createResp.charge_id) throw new Error('no_charge_id');

                showMsg('💳 جاري فتح نافذة الدفع...', 'info');

                if (tg && tg.requestPayment) {
                    tg.requestPayment({
                        currency: "XTR",
                        prices: [{ label: createResp.title, amount: amount }],
                        title: createResp.title,
                        description: createResp.description,
                        payload: createResp.charge_id,
                        provider_token: "",
                        start_parameter: "",
                        photo_url: "",
                        photo_size: 0,
                        photo_width: 0,
                        photo_height: 0,
                        need_name: false,
                        need_phone_number: false,
                        need_email: false,
                        need_shipping_address: false,
                        send_phone_number_to_provider: false,
                        send_email_to_provider: false,
                        is_flexible: false
                    }, (status) => {
                        if (status === "paid" || status === "success") {
                            completePayment(userId, amount, createResp.charge_id, btn);
                        } else if (status === "failed") {
                            showMsg('❌ فشل الدفع', 'error');
                            btn.disabled = false;
                            btn.textContent = '💛 ' + amount;
                        } else if (status === "cancelled") {
                            showMsg('❌ تم إلغاء العملية', 'error');
                            btn.disabled = false;
                            btn.textContent = '💛 ' + amount;
                        }
                    });
                } else {
                    showMsg('❌ الدفع غير متاح في هذه البيئة', 'error');
                    btn.disabled = false;
                    btn.textContent = '💛 ' + amount;
                }
            } catch (e) {
                showMsg('❌ خطأ: ' + (e.message || 'unknown'), 'error');
                btn.disabled = false;
                btn.textContent = '💛 ' + amount;
            }
        }

        async function completePayment(userId, amount, chargeId, btn) {
            try {
                showMsg('⏳ جاري تأكيد العملية...', 'info');
                
                let completeResp = await apiCall('/api/complete-payment', 'POST', {
                    user_id: userId,
                    amount: amount,
                    charge_id: chargeId
                });

                if (completeResp.success) {
                    showMsg('💛 ' + completeResp.message, 'success');
                    statsCache = null;
                    setTimeout(() => loadHome(), 1500);
                } else {
                    showMsg('⚠️ ' + (completeResp.message || 'خطأ غير معروف'), 'error');
                }
            } catch (e) {
                showMsg('❌ خطأ في التأكيد: ' + (e.message || 'unknown'), 'error');
            } finally {
                if (btn) {
                    btn.disabled = false;
                    btn.textContent = '💛 ' + amount;
                }
            }
        }

        function showMsg(text, type) {
            let m = document.createElement('div');
            m.className = `message ${type}`;
            m.textContent = text;
            document.getElementById('messages').appendChild(m);
            setTimeout(() => m.remove(), 3500);
        }

        if (userId) loadHome();
        else showMsg('⚠️ لم يتم تحميل بيانات المستخدم', 'error');
    </script>
</body>
</html>"""
    return make_response(html, 200, {'Content-Type': 'text/html; charset=utf-8'})


def set_webhook():
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if not domain or not BOT_TOKEN:
        return
    url = f"https://{domain}/webhook/{BOT_TOKEN}"
    try:
        requests.get(
            f"{TELEGRAM_API}/setWebhook",
            params={
                "url": url,
                "allowed_updates": json.dumps(["message", "pre_checkout_query"]),
            },
            timeout=10
        )
        print(f"✅ Webhook: {url}")
    except Exception as e:
        print(f"Webhook error: {e}")


init_db()
init_pool()
set_webhook()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
