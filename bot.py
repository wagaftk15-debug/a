import os
import json
import requests
import psycopg2
from flask import Flask, request, jsonify
from datetime import datetime

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


# ───────────────────────── تخزين البيانات (PostgreSQL) ─────────────────────────
def get_connection():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """ينشئ الجداول المطلوبة إذا لم تكن موجودة"""
    if not DATABASE_URL:
        print("DATABASE_URL غير موجود")
        return
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS registrations (
                user_id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS donations (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                amount INTEGER NOT NULL,
                telegram_payment_charge_id TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS suggestions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                content TEXT,
                telegram_payment_charge_id TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_actions (
                user_id BIGINT PRIMARY KEY,
                action TEXT NOT NULL,
                charge_id TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_points (
                user_id BIGINT PRIMARY KEY,
                total_points INTEGER NOT NULL DEFAULT 0,
                last_claim_date DATE
            )
            """
        )

        # جديد: جدول لمتابعة حالة الدفعات من الويب
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS web_payments (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                type TEXT NOT NULL,
                amount INTEGER NOT NULL,
                telegram_payment_charge_id TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        conn.commit()
        cur.close()
        conn.close()
        print("✅ تم تجهيز قاعدة البيانات بنجاح")
    except Exception as e:
        print(f"❌ فشل تجهيز قاعدة البيانات: {e}")


def upsert_user(user_id, username=None, first_name=None):
    """يحفظ/يحدّث بيانات المستخدم"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (user_id, username, first_name, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                updated_at = NOW()
            """,
            (user_id, username, first_name),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"upsert_user error: {e}")


def get_count():
    """عدد المسجلين"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM registrations")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"get_count error: {e}")
        return 0


def is_registered(user_id):
    """التحقق من التسجيل"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM registrations WHERE user_id = %s", (user_id,))
        exists = cur.fetchone() is not None
        cur.close()
        conn.close()
        return exists
    except Exception as e:
        print(f"is_registered error: {e}")
        return False


def register_user(user_id):
    """تسجيل المستخدم"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO registrations (user_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (user_id,),
        )
        added = cur.rowcount > 0
        conn.commit()
        cur.close()
        conn.close()
        return added
    except Exception as e:
        print(f"register_user error: {e}")
        return False


def record_donation(user_id, amount, charge_id):
    """تسجيل عملية دعم"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO donations (user_id, amount, telegram_payment_charge_id)
            VALUES (%s, %s, %s)
            """,
            (user_id, amount, charge_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"record_donation error: {e}")
        return False


def get_donations_stats():
    """إحصائيات الدعم"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM donations")
        count, total = cur.fetchone()
        cur.close()
        conn.close()
        return count, total
    except Exception as e:
        print(f"get_donations_stats error: {e}")
        return 0, 0


def record_suggestion(user_id, content, charge_id):
    """تسجيل اقتراح"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO suggestions (user_id, content, telegram_payment_charge_id)
            VALUES (%s, %s, %s)
            """,
            (user_id, content, charge_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"record_suggestion error: {e}")
        return False


def get_suggestions_count():
    """عدد الاقتراحات"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM suggestions")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"get_suggestions_count error: {e}")
        return 0


def unregister_user(user_id):
    """إلغاء التسجيل"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM registrations WHERE user_id = %s", (user_id,))
        removed = cur.rowcount > 0
        conn.commit()
        cur.close()
        conn.close()
        return removed
    except Exception as e:
        print(f"unregister_user error: {e}")
        return False


def claim_daily_points(user_id):
    """مطالبة النقاط اليومية"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO user_points (user_id, total_points, last_claim_date)
            VALUES (%s, %s, CURRENT_DATE)
            ON CONFLICT (user_id) DO UPDATE
            SET total_points = user_points.total_points + %s,
                last_claim_date = CURRENT_DATE
            WHERE user_points.last_claim_date IS DISTINCT FROM CURRENT_DATE
            RETURNING total_points
            """,
            (user_id, DAILY_POINTS, DAILY_POINTS),
        )
        row = cur.fetchone()
        if row:
            conn.commit()
            cur.close()
            conn.close()
            return True, row[0]

        cur.execute("SELECT total_points FROM user_points WHERE user_id = %s", (user_id,))
        existing = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return False, (existing[0] if existing else 0)
    except Exception as e:
        print(f"claim_daily_points error: {e}")
        return False, 0


def get_user_points(user_id):
    """الحصول على نقاط المستخدم"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT total_points FROM user_points WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else 0
    except Exception as e:
        print(f"get_user_points error: {e}")
        return 0


def get_leaderboard(limit=10):
    """لائحة الصدارة"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                p.user_id,
                COALESCE(NULLIF(u.username, ''), NULLIF(u.first_name, ''), 'مستخدم') AS display_name,
                p.total_points
            FROM user_points p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.total_points > 0
            ORDER BY p.total_points DESC, p.user_id ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [{"user_id": r[0], "name": r[1], "points": r[2]} for r in rows]
    except Exception as e:
        print(f"get_leaderboard error: {e}")
        return []


def mask_name(name):
    """تعتيم الاسم (يبقى أول حرف ظاهر)"""
    if not name:
        return "*"
    name = str(name).strip()
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


# ───────────────────────── دوال تيليجرام ─────────────────────────
def send_message(chat_id, text, reply_markup=None):
    """إرسال رسالة"""
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    try:
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        print(f"send_message error: {e}")


def answer_callback(callback_id, text, show_alert=False):
    """الرد على زر"""
    try:
        requests.post(
            f"{TELEGRAM_API}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text, "show_alert": show_alert},
            timeout=10,
        )
    except Exception as e:
        print(f"answer_callback error: {e}")


def send_invoice(chat_id, amount, title, description, payload_str):
    """إرسال فاتورة"""
    payload = {
        "chat_id": chat_id,
        "title": title,
        "description": description,
        "payload": payload_str,
        "provider_token": "",
        "currency": "XTR",
        "prices": [{"label": title, "amount": amount}],
    }
    try:
        r = requests.post(f"{TELEGRAM_API}/sendInvoice", json=payload, timeout=10)
        print(f"send_invoice({payload_str}) -> {r.json()}")
    except Exception as e:
        print(f"send_invoice error: {e}")


def answer_pre_checkout(pre_checkout_query_id, ok=True, error_message=None):
    """الرد على طلب ما قبل الدفع"""
    payload = {"pre_checkout_query_id": pre_checkout_query_id, "ok": ok}
    if error_message:
        payload["error_message"] = error_message
    try:
        requests.post(f"{TELEGRAM_API}/answerPreCheckoutQuery", json=payload, timeout=10)
    except Exception as e:
        print(f"answer_pre_checkout error: {e}")


def donation_keyboard():
    """لوحة مفاتيح الدعم"""
    keyboard = [
        [{"text": f"⭐ دعم بـ {amount} نجمة", "callback_data": f"donate_{amount}"}]
        for amount in DONATION_AMOUNTS
    ]
    return {"inline_keyboard": keyboard}


def main_keyboard():
    """اللوحة الرئيسية"""
    return {
        "inline_keyboard": [
            [{"text": "✅ بدي أتزوج", "callback_data": "want_marry"}],
            [
                {"text": "🎁 نقاط اليوم", "callback_data": "claim_points"},
                {"text": "🏆 الصدارة", "callback_data": "show_leaderboard"},
            ],
            [{"text": "💻 فتح الموقع", "web_app": {"url": get_site_url() or "https://t.me"}}],
            [{"text": "❌ إلغاء الاشتراك", "callback_data": "unsubscribe"}],
        ]
    }


def build_leaderboard_text():
    """بناء نص لائحة الصدارة"""
    top = get_leaderboard(10)
    if not top:
        return "لسا محدا كسب نقاط 🙂"
    medals = ["🥇", "🥈", "🥉"]
    lines = ["🏆 <b>لائحة الصدارة بالنقاط:</b>\n"]
    for i, u in enumerate(top):
        rank_icon = medals[i] if i < len(medals) else f"{i + 1}."
        lines.append(f"{rank_icon} {mask_name(u['name'])} — {u['points']} نقطة")
    return "\n".join(lines)


def notify_admin_new_suggestion(user_id, username, content):
    """إشعار الأدمن باقتراح جديد"""
    if not ADMIN_CHAT_ID:
        return
    who = f"@{username}" if username else f"id:{user_id}"
    send_message(
        ADMIN_CHAT_ID,
        f"💡 <b>اقتراح جديد من</b> {who}\n\n{content}",
    )


# ───────────────────────── الويب هوك (Webhook) ─────────────────────────
@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    update = request.get_json(force=True, silent=True) or {}

    # طلب تأكيد ما قبل الدفع
    if "pre_checkout_query" in update:
        pcq = update["pre_checkout_query"]
        answer_pre_checkout(pcq["id"], ok=True)
        return jsonify({"ok": True})

    # رسالة نصية أو دفعة
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        text = msg.get("text", "") or ""

        sender = msg.get("from", {})
        user_id = sender.get("id")
        if user_id:
            upsert_user(user_id, username=sender.get("username"), first_name=sender.get("first_name"))

        # دفعة ناجحة
        if "successful_payment" in msg:
            sp = msg["successful_payment"]
            amount = sp.get("total_amount", 0)
            charge_id = sp.get("telegram_payment_charge_id")
            invoice_payload = sp.get("invoice_payload", "") or ""

            if invoice_payload.startswith("suggest_"):
                send_message(
                    chat_id,
                    "✅ تم الدفع بنجاح 💡\nهلأ اكتبلنا فكرتك:",
                )
            else:
                record_donation(user_id, amount, charge_id)
                send_message(
                    chat_id,
                    f"💛 شكراً إلك!\nتم استلام دعمك: <b>{amount} نجمة</b> ⭐️",
                )
            return jsonify({"ok": True})

        if text.startswith("/start"):
            keyboard = main_keyboard()
            send_message(
                chat_id,
                "👋 أهلاً فيك في <b>بوت زوجوني</b> 💍\n\nاختر ما يناسبك:",
                keyboard,
            )

        elif text in ("/عدد", "/count"):
            send_message(chat_id, f"<b>عدد المسجلين:</b> {get_count()} 💍")

        elif text in ("/نقاطي", "/points"):
            send_message(chat_id, f"<b>مجموع نقاطك:</b> {get_user_points(user_id)} ⭐")

        elif text in ("/الصدارة", "/leaderboard"):
            send_message(chat_id, build_leaderboard_text())

        elif text in ("/انسحب", "/unsubscribe"):
            if is_registered(user_id):
                send_message(
                    chat_id,
                    "متأكد بدك تنسحب؟",
                    {
                        "inline_keyboard": [
                            [{"text": "✅ نعم", "callback_data": "confirm_unsubscribe"}],
                            [{"text": "🙅 لأ", "callback_data": "cancel_unsubscribe"}],
                        ]
                    },
                )
            else:
                send_message(chat_id, "أنت مش مسجل 🙂")

    # ضغطة على زر
    elif "callback_query" in update:
        cq = update["callback_query"]
        sender = cq.get("from", {})
        user_id = sender["id"]
        chat_id = cq["message"]["chat"]["id"]
        callback_id = cq["id"]
        data_key = cq.get("data", "")

        upsert_user(user_id, username=sender.get("username"), first_name=sender.get("first_name"))

        if data_key == "want_marry":
            if is_registered(user_id):
                answer_callback(callback_id, "أنت مسجل مسبقاً 😄")
            else:
                if register_user(user_id):
                    answer_callback(callback_id, "✅ تم تسجيلك!")
                    send_message(
                        chat_id,
                        f"🎉 تم تسجيلك بنجاح!\n<b>المسجلون الآن:</b> {get_count()} 💍",
                    )

        elif data_key == "claim_points":
            success, total = claim_daily_points(user_id)
            if success:
                answer_callback(callback_id, f"🎁 +{DAILY_POINTS} نقطة!")
                send_message(
                    chat_id,
                    f"🎉 أخدت {DAILY_POINTS} نقطة اليوم!\n<b>مجموعك الآن:</b> {total} ⭐",
                )
            else:
                answer_callback(callback_id, "أخدت نقاطك اليوم مسبقاً 🙏", show_alert=True)

        elif data_key == "show_leaderboard":
            answer_callback(callback_id, "")
            send_message(chat_id, build_leaderboard_text())

        elif data_key == "unsubscribe":
            if is_registered(user_id):
                send_message(
                    chat_id,
                    "متأكد؟",
                    {
                        "inline_keyboard": [
                            [{"text": "✅ نعم", "callback_data": "confirm_unsubscribe"}],
                            [{"text": "🙅 لأ", "callback_data": "cancel_unsubscribe"}],
                        ]
                    },
                )

        elif data_key == "confirm_unsubscribe":
            if unregister_user(user_id):
                answer_callback(callback_id, "✅ تم الانسحاب")
                send_message(chat_id, f"👋 تم حذفك\n<b>المسجلون الآن:</b> {get_count()} 💍")

        elif data_key == "cancel_unsubscribe":
            answer_callback(callback_id, "👍 تم التراجع")

    return jsonify({"ok": True})


# ───────────────────────── API للموقع الويب ─────────────────────────
@app.route("/api/user/info", methods=["POST"])
def api_user_info():
    """الحصول على معلومات المستخدم من البوت"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    
    if not user_id:
        return jsonify({"error": "user_id required"}), 400
    
    try:
        return jsonify({
            "user_id": user_id,
            "is_registered": is_registered(user_id),
            "points": get_user_points(user_id),
            "donation_count": get_donations_stats()[0],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/register", methods=["POST"])
def api_register():
    """تسجيل المستخدم من الموقع"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    
    if not user_id:
        return jsonify({"error": "user_id required"}), 400
    
    try:
        result = register_user(user_id)
        return jsonify({
            "success": result,
            "message": "تم التسجيل بنجاح 🎉" if result else "أنت مسجل مسبقاً"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/donate", methods=["POST"])
def api_donate():
    """معالجة الدفع من الموقع"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    amount = data.get("amount")
    charge_id = data.get("charge_id", "web_" + str(datetime.now().timestamp()))
    
    if not user_id or not amount:
        return jsonify({"error": "user_id and amount required"}), 400
    
    try:
        record_donation(user_id, amount, charge_id)
        return jsonify({
            "success": True,
            "message": f"شكراً لدعمك 💛 {amount} نجمة ⭐️"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stats", methods=["GET"])
def api_stats():
    """احصائيات عامة"""
    try:
        count, total = get_donations_stats()
        return jsonify({
            "registered": get_count(),
            "donations_count": count,
            "donations_total": total,
            "suggestions": get_suggestions_count(),
            "leaderboard": get_leaderboard(10),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/claim-points", methods=["POST"])
def api_claim_points():
    """مطالبة النقاط من الموقع"""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    
    if not user_id:
        return jsonify({"error": "user_id required"}), 400
    
    try:
        success, total = claim_daily_points(user_id)
        return jsonify({
            "success": success,
            "points": total,
            "message": f"+{DAILY_POINTS} نقطة!" if success else "أخدت نقاطك اليوم مسبقاً"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ───────────────────────── الموقع الويب (HTML) ─────────────────────────
@app.route("/")
def site_home():
    """الصفحة الرئيسية"""
    site_url = get_site_url()
    return """<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>بوت زوجوني 💍</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: 'Segoe UI', Tahoma, Arial, sans-serif;
            background: linear-gradient(135deg, #1e1a2e 0%, #16213e 100%);
            color: #fff;
            min-height: 100vh;
            padding: 12px;
        }
        .container { max-width: 500px; margin: 0 auto; }
        .header {
            text-align: center;
            padding: 20px 0;
            border-bottom: 2px solid rgba(255,255,255,0.1);
            margin-bottom: 20px;
        }
        .logo { font-size: 48px; margin: 0; }
        h1 {
            font-size: 24px;
            margin: 8px 0 0;
            background: linear-gradient(90deg, #ff5a8a, #7b3fe4);
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
        }
        .tabs {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            margin-bottom: 20px;
        }
        .tab-btn {
            padding: 12px;
            background: rgba(255,255,255,0.06);
            border: 2px solid rgba(255,255,255,0.1);
            color: #fff;
            border-radius: 10px;
            cursor: pointer;
            font-size: 14px;
            font-weight: bold;
            transition: all 0.3s;
        }
        .tab-btn.active {
            background: linear-gradient(90deg, #ff5a8a, #7b3fe4);
            border-color: #ff5a8a;
        }
        .tab-content {
            display: none;
            animation: slideIn 0.3s ease;
        }
        .tab-content.active { display: block; }
        @keyframes slideIn { from { opacity: 0; } to { opacity: 1; } }
        
        .card {
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 16px;
            padding: 18px;
            margin-bottom: 15px;
        }
        .stat { display: flex; justify-content: space-between; padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.06); }
        .stat:last-child { border-bottom: none; }
        .stat-label { color: #a0a0b0; font-size: 14px; }
        .stat-value { font-weight: bold; color: #ff5a8a; }
        
        .btn {
            width: 100%;
            padding: 14px;
            background: linear-gradient(90deg, #ff5a8a, #7b3fe4);
            color: #fff;
            border: none;
            border-radius: 10px;
            font-weight: bold;
            font-size: 15px;
            cursor: pointer;
            transition: transform 0.2s;
            margin-bottom: 10px;
        }
        .btn:active { transform: scale(0.98); }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        
        .amount-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            margin-bottom: 15px;
        }
        .amount-btn {
            padding: 12px;
            background: rgba(255,255,255,0.06);
            border: 2px solid rgba(255,255,255,0.1);
            color: #fff;
            border-radius: 10px;
            cursor: pointer;
            font-weight: bold;
            transition: all 0.3s;
        }
        .amount-btn.selected {
            background: #ff5a8a;
            border-color: #ff5a8a;
        }
        
        .leaderboard { list-style: none; padding: 0; margin: 0; }
        .lb-item {
            display: flex;
            align-items: center;
            padding: 12px;
            background: rgba(255,255,255,0.03);
            border-radius: 10px;
            margin-bottom: 8px;
        }
        .lb-rank { font-size: 20px; margin-right: 12px; }
        .lb-info { flex: 1; }
        .lb-name { font-weight: bold; }
        .lb-points { color: #ff5a8a; font-weight: bold; }
        
        .loading { text-align: center; padding: 20px; color: #a0a0b0; }
        .message {
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 15px;
            text-align: center;
            animation: slideIn 0.3s ease;
        }
        .message.success { background: rgba(76, 175, 80, 0.2); color: #4caf50; }
        .message.error { background: rgba(244, 67, 54, 0.2); color: #f44336; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo">💍</div>
            <h1>بوت زوجوني</h1>
        </div>

        <div class="tabs">
            <button class="tab-btn active" onclick="switchTab('home')">🏠 الرئيسية</button>
            <button class="tab-btn" onclick="switchTab('leaderboard')">🏆 الصدارة</button>
            <button class="tab-btn" onclick="switchTab('donate')">⭐ ادعم</button>
            <button class="tab-btn" onclick="switchTab('profile')">👤 ملفي</button>
        </div>

        <div id="home" class="tab-content active">
            <div class="card">
                <div id="homeStats">جاري التحميل...</div>
            </div>
            <button class="btn" onclick="registerUser()">✅ بدي أتزوج</button>
            <button class="btn" onclick="claimPoints()">🎁 خد نقاطك اليوم</button>
        </div>

        <div id="leaderboard" class="tab-content">
            <div class="card">
                <ul class="leaderboard" id="leaderboardList">
                    <div class="loading">جاري التحميل...</div>
                </ul>
            </div>
        </div>

        <div id="donate" class="tab-content">
            <div class="card">
                <h3 style="margin: 0 0 15px; text-align: center;">اختر المبلغ</h3>
                <div class="amount-grid">
                    <button class="amount-btn" onclick="selectAmount(100)">100 ⭐</button>
                    <button class="amount-btn" onclick="selectAmount(200)">200 ⭐</button>
                    <button class="amount-btn" onclick="selectAmount(500)">500 ⭐</button>
                    <button class="amount-btn" onclick="selectAmount(1000)">1000 ⭐</button>
                </div>
            </div>
            <button class="btn" id="donateBtn" onclick="sendDonation()" disabled>إرسال الدفع</button>
        </div>

        <div id="profile" class="tab-content">
            <div class="card">
                <div id="profileStats">جاري التحميل...</div>
            </div>
        </div>

        <div id="messages"></div>
    </div>

    <script>
        let tg = window.Telegram.WebApp;
        let selectedAmount = null;
        let user = tg.initData ? JSON.parse(decodeURIComponent(tg.initData)) : { user: { id: 0 } };
        let userId = user.user?.id || 0;

        tg.ready();
        tg.expand();

        function switchTab(tab) {
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
            
            document.getElementById(tab).classList.add('active');
            event.target.classList.add('active');
            
            if (tab === 'leaderboard') loadLeaderboard();
            if (tab === 'profile') loadProfile();
            if (tab === 'home') loadHome();
        }

        function loadHome() {
            fetch('/api/stats').then(r => r.json()).then(data => {
                document.getElementById('homeStats').innerHTML = `
                    <div class="stat"><span class="stat-label">المسجلون</span><span class="stat-value">${data.registered} 💍</span></div>
                    <div class="stat"><span class="stat-label">نجوم الدعم</span><span class="stat-value">${data.donations_total} ⭐</span></div>
                    <div class="stat"><span class="stat-label">الاقتراحات</span><span class="stat-value">${data.suggestions} 💡</span></div>
                `;
            });
        }

        function loadLeaderboard() {
            fetch('/api/stats').then(r => r.json()).then(data => {
                let html = '';
                let medals = ['🥇', '🥈', '🥉'];
                data.leaderboard.forEach((item, i) => {
                    html += `
                        <li class="lb-item">
                            <span class="lb-rank">${medals[i] || i + 1}</span>
                            <div class="lb-info">
                                <div class="lb-name">${item.name}</div>
                                <div class="lb-points">${item.points} نقطة</div>
                            </div>
                        </li>
                    `;
                });
                document.getElementById('leaderboardList').innerHTML = html || '<div class="loading">لا توجد بيانات</div>';
            });
        }

        function loadProfile() {
            fetch('/api/user/info', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ user_id: userId }) })
                .then(r => r.json()).then(data => {
                    document.getElementById('profileStats').innerHTML = `
                        <div class="stat"><span class="stat-label">ID</span><span class="stat-value">${data.user_id}</span></div>
                        <div class="stat"><span class="stat-label">الحالة</span><span class="stat-value">${data.is_registered ? '✅ مسجل' : '❌ غير مسجل'}</span></div>
                        <div class="stat"><span class="stat-label">النقاط</span><span class="stat-value">${data.points} ⭐</span></div>
                    `;
                });
        }

        function registerUser() {
            fetch('/api/register', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ user_id: userId }) })
                .then(r => r.json()).then(data => showMessage(data.message, data.success ? 'success' : 'error'));
        }

        function claimPoints() {
            fetch('/api/claim-points', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ user_id: userId }) })
                .then(r => r.json()).then(data => showMessage(data.message, data.success ? 'success' : 'error'));
        }

        function selectAmount(amount) {
            selectedAmount = amount;
            document.querySelectorAll('.amount-btn').forEach(btn => btn.classList.remove('selected'));
            event.target.classList.add('selected');
            document.getElementById('donateBtn').disabled = false;
        }

        function sendDonation() {
            if (!selectedAmount) return;
            fetch('/api/donate', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ user_id: userId, amount: selectedAmount }) })
                .then(r => r.json()).then(data => showMessage(data.message, data.success ? 'success' : 'error'));
        }

        function showMessage(text, type) {
            let msg = document.createElement('div');
            msg.className = `message ${type}`;
            msg.textContent = text;
            document.getElementById('messages').appendChild(msg);
            setTimeout(() => msg.remove(), 3000);
        }

        loadHome();
    </script>
</body>
</html>
    """


# ───────────────────────── تفعيل الويب هوك ─────────────────────────
def set_webhook():
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if not domain or not BOT_TOKEN:
        print("⚠️ RAILWAY_PUBLIC_DOMAIN أو BOT_TOKEN غير موجودين")
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
set_webhook()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
