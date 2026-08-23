import os
import json
import requests
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# التوكن يُقرأ من متغير بيئة اسمه BOT_TOKEN (تضيفه من لوحة Railway)
BOT_TOKEN = os.environ.get("8667627503:AAF3g679L4g5IxIS6s8Dhf7QM-tWug8_kFk", "")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DATA_FILE = os.path.join(os.path.dirname(__file__), "data.json")


# ───────────────────────── تخزين البيانات ─────────────────────────
def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"count": 0, "user_ids": []}


def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


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


# ───────────────────────── صفحة الويب ─────────────────────────
@app.route("/")
def home():
    data = load_data()
    return render_template("index.html", count=data["count"])


@app.route("/api/count")
def api_count():
    data = load_data()
    return jsonify({"count": data["count"]})


# ───────────────────────── الويب هوك (استقبال رسائل البوت) ─────────────────────────
@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    update = request.get_json(force=True, silent=True) or {}

    # رسالة نصية عادية (مثل /start)
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        text = msg.get("text", "")

        if text == "/start":
            keyboard = {
                "inline_keyboard": [
                    [{"text": "✅ بدي أتزوج", "callback_data": "want_marry"}]
                ]
            }
            send_message(
                chat_id,
                "أهلاً فيك في بوت زوجوني 💍\n"
                "اضغط الزر تحت إذا بدك تسجل حالك ضمن قائمة المستعدين للزواج 😄",
                keyboard,
            )

        elif text in ("/عدد", "/count"):
            data = load_data()
            send_message(chat_id, f"عدد الأشخاص المسجلين لحد الآن: {data['count']} 💍")

    # ضغطة على الزر (Callback Query)
    elif "callback_query" in update:
        cq = update["callback_query"]
        user_id = cq["from"]["id"]
        chat_id = cq["message"]["chat"]["id"]
        callback_id = cq["id"]
        data_key = cq.get("data")

        if data_key == "want_marry":
            data = load_data()
            if user_id in data["user_ids"]:
                answer_callback(callback_id, "أنت مسجل مسبقاً 😄")
            else:
                data["user_ids"].append(user_id)
                data["count"] += 1
                save_data(data)
                answer_callback(callback_id, "تم تسجيلك! مبروك مقدماً 🎉")
                send_message(
                    chat_id,
                    f"تم تسجيلك بنجاح ✅\nعدد الراغبين بالزواج لحد الآن: {data['count']} 💍",
                )

    return jsonify({"ok": True})


# ───────────────────────── تفعيل الويب هوك تلقائياً ─────────────────────────
def set_webhook():
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if not domain or not BOT_TOKEN:
        print("BOT_TOKEN أو RAILWAY_PUBLIC_DOMAIN غير موجودين، لن يتم ضبط الويب هوك تلقائياً.")
        return
    url = f"https://{domain}/webhook/{BOT_TOKEN}"
    try:
        r = requests.get(f"{TELEGRAM_API}/setWebhook", params={"url": url}, timeout=10)
        print(f"Webhook set to: {url} -> {r.json()}")
    except Exception as e:
        print(f"فشل ضبط الويب هوك: {e}")


# يُنفَّذ عند استيراد الملف (يعمل مع gunicorn والتشغيل المباشر على حد سواء)
set_webhook()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
