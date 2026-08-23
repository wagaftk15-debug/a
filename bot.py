import os
import json
import requests
import psycopg2
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)

# التوكن يُقرأ من متغير بيئة اسمه BOT_TOKEN (تُضاف من لوحة Railway → Variables)
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# رابط قاعدة البيانات (يُضاف من لوحة Railway → Variables باسم DATABASE_URL)
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# قيم الدعم المتاحة بنجوم تيليجرام
DONATION_AMOUNTS = [100, 200, 500, 1000]


# ───────────────────────── تخزين البيانات (PostgreSQL) ─────────────────────────
def get_connection():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """ينشئ الجداول المطلوبة إذا لم تكن موجودة، يُنفَّذ مرة عند بدء التطبيق."""
    if not DATABASE_URL:
        print("DATABASE_URL غير موجود، لن يتم الاتصال بقاعدة البيانات.")
        return
    try:
        conn = get_connection()
        cur = conn.cursor()
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
        conn.commit()
        cur.close()
        conn.close()
        print("تم تجهيز قاعدة البيانات بنجاح.")
    except Exception as e:
        print(f"فشل تجهيز قاعدة البيانات: {e}")


def get_count():
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
    """يسجل المستخدم ويرجع True لو نجح، False لو كان مسجل مسبقاً."""
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
    """يسجل عملية دعم ناجحة في جدول donations."""
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
    """يرجع (عدد عمليات الدعم، مجموع النجوم) من جدول donations."""
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


# ───────────────────────── دوال تيليجرام ─────────────────────────
def send_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    try:
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        print(f"send_message error: {e}")


def answer_callback(callback_id, text):
    try:
        requests.post(
            f"{TELEGRAM_API}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text, "show_alert": False},
            timeout=10,
        )
    except Exception as e:
        print(f"answer_callback error: {e}")


def send_invoice(chat_id, amount):
    """يرسل فاتورة دفع بنجوم تيليجرام (XTR). provider_token يجب أن يكون فارغاً للنجوم."""
    payload = {
        "chat_id": chat_id,
        "title": "دعم بوت زوجوني 💍",
        "description": f"دعمك بقيمة {amount} نجمة تيليجرام بيساعدنا نكمل ونطوّر البوت 🙏",
        "payload": f"donate_{amount}_{chat_id}",
        "provider_token": "",  # فارغ إلزامياً عند استخدام عملة النجوم XTR
        "currency": "XTR",
        "prices": [{"label": f"دعم {amount} نجمة", "amount": amount}],
    }
    try:
        r = requests.post(f"{TELEGRAM_API}/sendInvoice", json=payload, timeout=10)
        print(f"send_invoice({amount}) -> {r.json()}")
    except Exception as e:
        print(f"send_invoice error: {e}")


def answer_pre_checkout(pre_checkout_query_id, ok=True, error_message=None):
    """يجب الرد على pre_checkout_query خلال 10 ثواني وإلا يُرفض الدفع تلقائياً."""
    payload = {"pre_checkout_query_id": pre_checkout_query_id, "ok": ok}
    if error_message:
        payload["error_message"] = error_message
    try:
        requests.post(f"{TELEGRAM_API}/answerPreCheckoutQuery", json=payload, timeout=10)
    except Exception as e:
        print(f"answer_pre_checkout error: {e}")


def donation_keyboard():
    return {
        "inline_keyboard": [
            [{"text": f"⭐ دعم بـ {amount} نجمة", "callback_data": f"donate_{amount}"}]
            for amount in DONATION_AMOUNTS
        ]
    }


# ───────────────────────── صفحة الويب (HTML مدمج بالكود مباشرة) ─────────────────────────
HTML_PAGE = """
<!DOCTYPE html>
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
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    background: linear-gradient(135deg, #1a1a2e, #16213e);
    font-family: 'Segoe UI', Tahoma, Arial, sans-serif;
    color: #f1f5f9;
  }
  .card {
    background: rgba(255, 255, 255, 0.05);
    border: 1px solid #2e3a55;
    border-radius: 24px;
    padding: 40px 50px;
    text-align: center;
    box-shadow: 0 20px 60px rgba(0, 0, 0, 0.4);
    max-width: 90vw;
  }
  h1 { font-size: 26px; margin: 0 0 6px 0; }
  .subtitle { color: #8a9bb8; font-size: 13px; margin-bottom: 20px; }
  .count {
    font-size: 64px;
    font-weight: bold;
    color: #c8a96e;
    margin: 10px 0;
    transition: transform 0.2s ease;
  }
  p.desc { color: #8a9bb8; font-size: 14px; margin: 0 0 20px 0; }
  a.btn {
    display: inline-block;
    padding: 12px 28px;
    background: #c8a96e;
    color: #1a1a2e;
    text-decoration: none;
    border-radius: 50px;
    font-weight: bold;
    transition: opacity 0.2s ease;
  }
  a.btn:hover { opacity: 0.85; }
</style>
</head>
<body>
  <div class="card">
    <h1>بوت زوجوني 💍</h1>
    <div class="subtitle">إحصائية حيّة من بوت التلقرام</div>
    <div class="count" id="count">{{ count }}</div>
    <p class="desc">شخص سجّل رغبته بالزواج حتى الآن</p>
    <a class="btn" href="https://t.me/zawjoni" target="_blank" rel="noopener">
      سجّل نفسك عبر البوت
    </a>
  </div>

  <script>
    // تفعيل وضع Mini App داخل تيليجرام (توسعة كاملة للشاشة)
    if (window.Telegram && window.Telegram.WebApp) {
      const tg = window.Telegram.WebApp;
      tg.ready();
      tg.expand();
    }

    async function refreshCount() {
      try {
        const res = await fetch('/api/count');
        const data = await res.json();
        const el = document.getElementById('count');
        if (el.textContent != data.count) {
          el.textContent = data.count;
          el.style.transform = 'scale(1.15)';
          setTimeout(() => { el.style.transform = 'scale(1)'; }, 200);
        }
      } catch (e) {
        console.error('تعذر تحديث العدد', e);
      }
    }
    setInterval(refreshCount, 5000);
  </script>
</body>
</html>
"""


@app.route("/")
def home():
    count = get_count()
    return render_template_string(HTML_PAGE, count=count)


@app.route("/api/count")
def api_count():
    return jsonify({"count": get_count()})


@app.route("/api/donations")
def api_donations():
    count, total = get_donations_stats()
    return jsonify({"donations_count": count, "stars_total": total})


# ───────────────────────── الويب هوك (استقبال رسائل البوت) ─────────────────────────
@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    update = request.get_json(force=True, silent=True) or {}

    # طلب تأكيد ما قبل الدفع - إلزامي الرد عليه خلال 10 ثواني
    if "pre_checkout_query" in update:
        pcq = update["pre_checkout_query"]
        # هون ممكن تضيف تحقق إضافي إذا حبيت (مثلاً التأكد إن المبلغ ضمن القيم المسموحة)
        answer_pre_checkout(pcq["id"], ok=True)
        return jsonify({"ok": True})

    # رسالة نصية عادية (مثل /start) أو إشعار دفع ناجح
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        text = msg.get("text", "")

        # دفعة ناجحة بنجوم تيليجرام
        if "successful_payment" in msg:
            sp = msg["successful_payment"]
            amount = sp.get("total_amount", 0)  # بعملة XTR القيمة = عدد النجوم مباشرة
            charge_id = sp.get("telegram_payment_charge_id")
            user_id = msg["from"]["id"]
            record_donation(user_id, amount, charge_id)
            send_message(
                chat_id,
                f"شكراً إلك من قلبنا 🙏💛\nتم استلام دعمك بقيمة {amount} نجمة ⭐️\nالله يجزيك الخير!",
            )
            return jsonify({"ok": True})

        if text == "/start":
            site_url = f"https://{os.environ.get('RAILWAY_PUBLIC_DOMAIN', '')}"
            keyboard = {
                "inline_keyboard": [
                    [{"text": "✅ بدي أتزوج", "callback_data": "want_marry"}],
                    [{"text": "🌐 ادخل", "web_app": {"url": site_url}}],
                    [{"text": "⭐ ادعم البوت", "callback_data": "show_donate"}],
                ]
            }
            send_message(
                chat_id,
                "أهلاً فيك في بوت زوجوني 💍\n"
                "اضغط الزر تحت إذا بدك تسجل حالك ضمن قائمة المستعدين للزواج 😄",
                keyboard,
            )

        elif text in ("/عدد", "/count"):
            send_message(chat_id, f"عدد الأشخاص المسجلين لحد الآن: {get_count()} 💍")

        elif text in ("/دعم", "/support", "/donate"):
            send_message(
                chat_id,
                "بتقدر تدعم البوت بنجوم تيليجرام ⭐ اختر القيمة اللي بتناسبك:",
                donation_keyboard(),
            )

        elif text in ("/احصائية_الدعم", "/donations"):
            count, total = get_donations_stats()
            send_message(
                chat_id,
                f"عدد عمليات الدعم: {count}\nإجمالي النجوم المستلمة: {total} ⭐️",
            )

    # ضغطة على الزر (Callback Query)
    elif "callback_query" in update:
        cq = update["callback_query"]
        user_id = cq["from"]["id"]
        chat_id = cq["message"]["chat"]["id"]
        callback_id = cq["id"]
        data_key = cq.get("data", "")

        if data_key == "want_marry":
            if is_registered(user_id):
                answer_callback(callback_id, "أنت مسجل مسبقاً 😄")
            else:
                added = register_user(user_id)
                if added:
                    answer_callback(callback_id, "تم تسجيلك! مبروك مقدماً 🎉")
                    send_message(
                        chat_id,
                        f"تم تسجيلك بنجاح ✅\nعدد الراغبين بالزواج لحد الآن: {get_count()} 💍",
                    )
                else:
                    answer_callback(callback_id, "حصل خطأ، جرب مرة ثانية 🙏")

        elif data_key == "show_donate":
            answer_callback(callback_id, "")
            send_message(
                chat_id,
                "بتقدر تدعم البوت بنجوم تيليجرام ⭐ اختر القيمة اللي بتناسبك:",
                donation_keyboard(),
            )

        elif data_key.startswith("donate_"):
            try:
                amount = int(data_key.split("_")[1])
            except (IndexError, ValueError):
                amount = 0
            if amount in DONATION_AMOUNTS:
                answer_callback(callback_id, f"جاري تجهيز فاتورة {amount} نجمة ⭐")
                send_invoice(chat_id, amount)
            else:
                answer_callback(callback_id, "قيمة غير صالحة 🙏")

    return jsonify({"ok": True})


# ───────────────────────── تفعيل الويب هوك تلقائياً ─────────────────────────
def set_webhook():
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if not domain or not BOT_TOKEN:
        print("BOT_TOKEN أو RAILWAY_PUBLIC_DOMAIN غير موجودين، لن يتم ضبط الويب هوك تلقائياً.")
        return
    url = f"https://{domain}/webhook/{BOT_TOKEN}"
    try:
        # allowed_updates لازم تتضمن pre_checkout_query عشان تيليجرام يرسلها للويب هوك
        r = requests.get(
            f"{TELEGRAM_API}/setWebhook",
            params={
                "url": url,
                "allowed_updates": json.dumps(
                    ["message", "callback_query", "pre_checkout_query"]
                ),
            },
            timeout=10,
        )
        print(f"Webhook set to: {url} -> {r.json()}")
    except Exception as e:
        print(f"فشل ضبط الويب هوك: {e}")


# يُنفَّذ عند استيراد الملف (يعمل مع gunicorn والتشغيل المباشر على حد سواء)
init_db()
set_webhook()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
