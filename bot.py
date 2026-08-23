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

# آيدي شات الأدمن (اختياري) عشان يوصله إشعار بكل اقتراح جديد
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")

# قيم الدعم المتاحة بنجوم تيليجرام
DONATION_AMOUNTS = [100, 200, 500, 1000]

# سعر اقتراح فكرة/ميزة جديدة بنجوم تيليجرام
SUGGESTION_PRICE = 100

# عدد نقاط المكافأة اليومية
DAILY_POINTS = 100


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

        # جدول عام لكل مستخدم تفاعل مع البوت (يخزن آخر اسم/يوزر معروف له)
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

        # اقتراحات الأفكار/الميزات المدفوعة
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

        # حالة انتظار: تتبع لكل مستخدم شو البوت مستني منه كخطوة جاية (مثلاً نص الاقتراح بعد الدفع)
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

        # نقاط المستخدمين (مكافأة يومية بالضغط على زر مخصص)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_points (
                user_id BIGINT PRIMARY KEY,
                total_points INTEGER NOT NULL DEFAULT 0,
                last_claim_date DATE
            )
            """
        )

        conn.commit()
        cur.close()
        conn.close()
        print("تم تجهيز قاعدة البيانات بنجاح.")
    except Exception as e:
        print(f"فشل تجهيز قاعدة البيانات: {e}")


def upsert_user(user_id, username=None, first_name=None):
    """يحفظ/يحدّث آخر اسم مستخدم معروف لكل شخص تفاعل مع البوت."""
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


def get_registered_users_with_donations():
    """
    يرجع قائمة المسجلين (طلب الزواج) مع اسم كل واحد (من جدول users)
    ومجموع ما تبرّع به من نجوم، مرتبة حسب تاريخ التسجيل.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                r.user_id,
                COALESCE(NULLIF(u.username, ''), NULLIF(u.first_name, ''), 'مستخدم') AS display_name,
                COALESCE(SUM(d.amount), 0) AS total_donated,
                r.created_at
            FROM registrations r
            LEFT JOIN users u ON u.user_id = r.user_id
            LEFT JOIN donations d ON d.user_id = r.user_id
            GROUP BY r.user_id, u.username, u.first_name, r.created_at
            ORDER BY r.created_at ASC
            """
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {"user_id": row[0], "name": row[1], "donated": int(row[2])}
            for row in rows
        ]
    except Exception as e:
        print(f"get_registered_users_with_donations error: {e}")
        return []


def mask_name(name):
    """
    يعتم كل حروف الاسم ما عدا الحرف الأول (يبقى ظاهر بدون تعتيم).
    مثال: "أحمد" -> "أ***"
    """
    if not name:
        return "*"
    name = str(name).strip()
    if len(name) <= 1:
        return name
    return name[0] + "*" * (len(name) - 1)


# ───────────────────────── اقتراحات الأفكار ─────────────────────────
def set_pending_action(user_id, action, charge_id=None):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pending_actions (user_id, action, charge_id, created_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET action = EXCLUDED.action,
                charge_id = EXCLUDED.charge_id,
                created_at = NOW()
            """,
            (user_id, action, charge_id),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"set_pending_action error: {e}")


def get_pending_action(user_id):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT action, charge_id FROM pending_actions WHERE user_id = %s",
            (user_id,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return {"action": row[0], "charge_id": row[1]}
        return None
    except Exception as e:
        print(f"get_pending_action error: {e}")
        return None


def clear_pending_action(user_id):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM pending_actions WHERE user_id = %s", (user_id,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"clear_pending_action error: {e}")


def record_suggestion(user_id, content, charge_id):
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


# ───────────────────────── إلغاء الاشتراك (الانسحاب) ─────────────────────────
def unregister_user(user_id):
    """يحذف المستخدم من جدول registrations. يرجع True لو كان مسجل وانحذف فعلاً."""
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


# ───────────────────────── نظام النقاط اليومية ─────────────────────────
def claim_daily_points(user_id):
    """
    يحاول منح المستخدم DAILY_POINTS نقطة لهاليوم.
    يرجع (True, المجموع_الجديد) لو نجحت العملية (أول مرة اليوم).
    يرجع (False, المجموع_الحالي) لو كان خد نقاطه اليوم مسبقاً.
    """
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

        # ما انحدث أي شيء يعني خد نقاطه اليوم مسبقاً، منجيب مجموعه الحالي
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
    """يرجع أعلى المستخدمين نقاطاً (اسم + مجموع نقاط)."""
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


# ───────────────────────── دوال تيليجرام ─────────────────────────
def send_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    try:
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        print(f"send_message error: {e}")


def answer_callback(callback_id, text, show_alert=False):
    try:
        requests.post(
            f"{TELEGRAM_API}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text, "show_alert": show_alert},
            timeout=10,
        )
    except Exception as e:
        print(f"answer_callback error: {e}")


def send_invoice(chat_id, amount, title, description, payload_str):
    """
    يرسل فاتورة دفع بنجوم تيليجرام (XTR). provider_token يجب أن يكون فارغاً للنجوم.
    payload_str بيتخزن جوا الفاتورة وبيرجع لنا بعد الدفع الناجح، بنستخدمه لنعرف
    نوع العملية (دعم عادي، أو شراء اقتراح فكرة).
    """
    payload = {
        "chat_id": chat_id,
        "title": title,
        "description": description,
        "payload": payload_str,
        "provider_token": "",  # فارغ إلزامياً عند استخدام عملة النجوم XTR
        "currency": "XTR",
        "prices": [{"label": title, "amount": amount}],
    }
    try:
        r = requests.post(f"{TELEGRAM_API}/sendInvoice", json=payload, timeout=10)
        print(f"send_invoice({payload_str}) -> {r.json()}")
    except Exception as e:
        print(f"send_invoice error: {e}")


def send_donation_invoice(chat_id, amount):
    send_invoice(
        chat_id,
        amount,
        "دعم بوت زوجوني 💍",
        f"دعمك بقيمة {amount} نجمة تيليجرام بيساعدنا نكمل ونطوّر البوت 🙏",
        f"donate_{amount}_{chat_id}",
    )


def send_suggestion_invoice(chat_id):
    send_invoice(
        chat_id,
        SUGGESTION_PRICE,
        "اقتراح فكرة جديدة 💡",
        f"ادفع {SUGGESTION_PRICE} نجمة وابعتلنا فكرتك/اقتراحك لتطوير البوت، وفريقنا بيراجعها بجدّية",
        f"suggest_{SUGGESTION_PRICE}_{chat_id}",
    )


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
    keyboard = [
        [{"text": f"⭐ دعم بـ {amount} نجمة", "callback_data": f"donate_{amount}"}]
        for amount in DONATION_AMOUNTS
    ]
    return {"inline_keyboard": keyboard}


def unsubscribe_confirm_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "✅ نعم، بدي انسحب", "callback_data": "confirm_unsubscribe"}],
            [{"text": "🙅 لأ، تراجعت", "callback_data": "cancel_unsubscribe"}],
        ]
    }


def build_leaderboard_text():
    top = get_leaderboard(10)
    if not top:
        return "لسا محدا كسب نقاط 🙂 كون أول واحد يجمع نقاط اليوم!"
    medals = ["🥇", "🥈", "🥉"]
    lines = ["🏆 لائحة الصدارة بالنقاط:\n"]
    for i, u in enumerate(top):
        rank_icon = medals[i] if i < len(medals) else f"{i + 1}."
        lines.append(f"{rank_icon} {mask_name(u['name'])} — {u['points']} نقطة")
    return "\n".join(lines)


def notify_admin_new_suggestion(user_id, username, content):
    if not ADMIN_CHAT_ID:
        return
    who = f"@{username}" if username else f"id:{user_id}"
    send_message(
        ADMIN_CHAT_ID,
        f"💡 اقتراح جديد من {who}\n\n{content}",
    )


# ───────────────────────── واجهة الويب (تطبيق مصغّر ملء الشاشة) ─────────────────────────
MINI_APP_PAGE = """
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover, user-scalable=no">
<title>زوجوني 💍</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Amiri:wght@400;700&family=Tajawal:wght@400;500;700;900&display=swap" rel="stylesheet">
<style>
  :root {
    --bg-1: #241a35;
    --bg-2: #120c1f;
    --card: rgba(247, 237, 224, 0.05);
    --card-line: rgba(247, 237, 224, 0.10);
    --cream: #f7ede0;
    --muted: #a89bc0;
    --blush: #e2a38f;
    --blush-deep: #c17d68;
  }
  * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
  html, body {
    height: 100%;
    margin: 0;
    overscroll-behavior: none;
  }
  body {
    background: radial-gradient(130% 95% at 50% -5%, var(--bg-1) 0%, var(--bg-2) 65%);
    font-family: 'Tajawal', 'Segoe UI', Tahoma, Arial, sans-serif;
    color: var(--cream);
  }

  /* ───── هيكل التطبيق ───── */
  .app {
    height: 100dvh;
    width: 100%;
    max-width: 480px;
    margin: 0 auto;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    padding-top: env(safe-area-inset-top, 0px);
    position: relative;
  }

  .glow {
    position: absolute;
    top: -120px; right: -80px;
    width: 320px; height: 320px;
    background: radial-gradient(circle, rgba(226,163,143,0.16), transparent 70%);
    pointer-events: none;
    z-index: 0;
  }

  /* ───── شريط علوي ───── */
  .topbar {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 9px;
    padding: 16px 16px 6px;
    flex-shrink: 0;
    position: relative;
    z-index: 1;
  }
  .topbar svg { width: 21px; height: 21px; color: var(--blush); flex-shrink: 0; }
  .topbar span {
    font-family: 'Amiri', serif;
    font-size: 19px;
    font-weight: 700;
    letter-spacing: 0.5px;
  }

  /* ───── الصفحات ───── */
  .view {
    display: none;
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    -webkit-overflow-scrolling: touch;
    padding: 8px 22px 28px;
    position: relative;
    z-index: 1;
    animation: fadeIn 0.35s ease;
  }
  .view.active { display: flex; flex-direction: column; }
  @keyframes fadeIn { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: translateY(0); } }

  /* --- صفحة الرئيسية --- */
  #view-home { align-items: center; justify-content: center; text-align: center; }
  .flourish { width: 128px; height: 14px; opacity: 0.6; margin: 0 auto 24px; display: block; }
  .hero-label {
    color: var(--muted);
    font-size: 13px;
    letter-spacing: 0.4px;
    margin-bottom: 10px;
  }
  .count {
    font-family: 'Amiri', serif;
    font-size: clamp(64px, 20vw, 92px);
    line-height: 1;
    font-weight: 700;
    color: var(--blush);
    margin: 0;
    transition: transform 0.25s ease;
  }
  .count-desc {
    color: var(--muted);
    font-size: 14px;
    line-height: 1.7;
    margin: 16px 0 0;
    max-width: 270px;
  }

  /* --- صفحة الداعمين --- */
  #view-donations { padding-top: 16px; }
  .donations-summary {
    display: flex;
    justify-content: space-around;
    background: var(--card);
    border: 1px solid var(--card-line);
    border-radius: 18px;
    padding: 18px 10px;
    margin-bottom: 18px;
    flex-shrink: 0;
  }
  .stat { text-align: center; }
  .stat b {
    display: block;
    font-family: 'Amiri', serif;
    font-size: 27px;
    color: var(--blush);
  }
  .stat span { font-size: 11px; color: var(--muted); }

  .section-label {
    font-size: 13px;
    color: var(--muted);
    margin: 4px 2px 10px;
  }
  .list {
    background: var(--card);
    border: 1px solid var(--card-line);
    border-radius: 18px;
    overflow: hidden;
  }
  .row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 14px 17px;
    border-bottom: 1px solid var(--card-line);
  }
  .row:last-child { border-bottom: none; }
  .row .name { font-size: 14.5px; letter-spacing: 0.5px; }
  .row .donated {
    font-size: 13.5px;
    font-weight: 700;
    color: var(--blush);
    white-space: nowrap;
  }
  .row .donated.zero { color: var(--muted); font-weight: 400; }
  .empty {
    text-align: center;
    color: var(--muted);
    font-size: 13.5px;
    padding: 44px 10px;
  }

  /* ───── قسم الأزرار السفلي (التنقّل) ───── */
  .tabbar {
    flex-shrink: 0;
    display: flex;
    gap: 10px;
    border-top: 1px solid var(--card-line);
    background: rgba(18, 12, 31, 0.88);
    backdrop-filter: blur(14px);
    -webkit-backdrop-filter: blur(14px);
    padding: 12px 16px calc(12px + env(safe-area-inset-bottom, 0px));
    position: relative;
    z-index: 1;
  }
  .tab {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 7px;
    background: none;
    border: 1px solid transparent;
    color: var(--muted);
    font-family: inherit;
    font-size: 13px;
    font-weight: 700;
    padding: 12px 8px;
    border-radius: 14px;
    cursor: pointer;
    transition: color 0.15s ease, background 0.15s ease, border-color 0.15s ease;
  }
  .tab svg { width: 19px; height: 19px; flex-shrink: 0; }
  .tab.active {
    color: var(--blush);
    background: rgba(226, 163, 143, 0.10);
    border-color: rgba(226, 163, 143, 0.25);
  }
</style>
</head>
<body>
  <div class="app">
    <div class="glow"></div>

    <div class="topbar">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6">
        <circle cx="9" cy="14" r="5.2"/>
        <circle cx="15" cy="14" r="5.2"/>
      </svg>
      <span>زوجوني</span>
    </div>

    <main id="view-home" class="view active">
      <svg class="flourish" viewBox="0 0 128 14" fill="none" stroke="#e2a38f" stroke-width="1">
        <line x1="0" y1="7" x2="48" y2="7"/>
        <circle cx="64" cy="7" r="4"/>
        <line x1="80" y1="7" x2="128" y2="7"/>
      </svg>
      <div class="hero-label">شخص سجّل رغبته بالزواج حتى الآن</div>
      <div class="count" id="count">{{ count }}</div>
      <div class="count-desc">القائمة بتزيد كل يوم، سجّل حالك وخلّي حظك يشتغل 😄</div>
    </main>

    <main id="view-donations" class="view">
      <div class="donations-summary">
        <div class="stat"><b id="d-count">0</b><span>عملية دعم</span></div>
        <div class="stat"><b id="d-total">0</b><span>نجمة إجمالي</span></div>
      </div>
      <div class="section-label">المسجلون</div>
      <div class="list" id="users-list">
        <div class="empty">جاري التحميل...</div>
      </div>
    </main>

    <nav class="tabbar">
      <button class="tab active" data-view="home">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
          <path d="M4 11.5 12 5l8 6.5"/><path d="M6 10.5V19h12v-8.5"/>
        </svg>
        الرئيسية
      </button>
      <button class="tab" data-view="donations">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
          <circle cx="12" cy="8" r="3.4"/><path d="M5 20c0-3.6 3-6.2 7-6.2s7 2.6 7 6.2"/>
        </svg>
        المستخدمون
      </button>
    </nav>
  </div>

  <script>
    if (window.Telegram && window.Telegram.WebApp) {
      const tg = window.Telegram.WebApp;
      tg.ready();
      tg.expand();
      if (tg.setHeaderColor) { try { tg.setHeaderColor('#120c1f'); } catch (e) {} }
      if (tg.setBackgroundColor) { try { tg.setBackgroundColor('#120c1f'); } catch (e) {} }
      if (tg.disableVerticalSwipes) { try { tg.disableVerticalSwipes(); } catch (e) {} }
    }

    const tabs = document.querySelectorAll('.tab');
    const views = { home: document.getElementById('view-home'), donations: document.getElementById('view-donations') };

    function showView(name) {
      Object.entries(views).forEach(([key, el]) => el.classList.toggle('active', key === name));
      tabs.forEach(t => t.classList.toggle('active', t.dataset.view === name));
      if (name === 'donations') loadDonations();
    }
    tabs.forEach(t => t.addEventListener('click', () => showView(t.dataset.view)));

    async function refreshCount() {
      try {
        const res = await fetch('/api/count');
        const data = await res.json();
        const el = document.getElementById('count');
        if (el.textContent != data.count) {
          el.textContent = data.count;
          el.style.transform = 'scale(1.12)';
          setTimeout(() => { el.style.transform = 'scale(1)'; }, 200);
        }
      } catch (e) { console.error('تعذر تحديث العدد', e); }
    }

    async function loadDonations() {
      try {
        const [statsRes, usersRes] = await Promise.all([fetch('/api/donations'), fetch('/api/users')]);
        const stats = await statsRes.json();
        const usersData = await usersRes.json();
        document.getElementById('d-count').textContent = stats.donations_count;
        document.getElementById('d-total').textContent = stats.stars_total;

        const list = document.getElementById('users-list');
        if (!usersData.users || usersData.users.length === 0) {
          list.innerHTML = '<div class="empty">ما في حدا مسجل لسا 🙂</div>';
          return;
        }
        list.innerHTML = usersData.users.map(u => `
          <div class="row">
            <span class="name">${u.masked_name}</span>
            <span class="donated ${u.donated === 0 ? 'zero' : ''}">${u.donated} ⭐</span>
          </div>
        `).join('');
      } catch (e) { console.error('تعذر تحميل قائمة الداعمين', e); }
    }

    refreshCount();
    setInterval(refreshCount, 5000);

    const params = new URLSearchParams(window.location.search);
    if (params.get('tab') === 'donations') showView('donations');
  </script>
</body>
</html>
"""


@app.route("/")
def home():
    count = get_count()
    return render_template_string(MINI_APP_PAGE, count=count, suggestion_price=SUGGESTION_PRICE)


@app.route("/users")
def users_page():
    """رابط قديم متوافق: نفس التطبيق مفتوح على تبويب الداعمين."""
    count = get_count()
    return render_template_string(MINI_APP_PAGE, count=count, suggestion_price=SUGGESTION_PRICE)


@app.route("/api/count")
def api_count():
    return jsonify({"count": get_count()})


@app.route("/api/donations")
def api_donations():
    count, total = get_donations_stats()
    return jsonify({"donations_count": count, "stars_total": total})


@app.route("/api/users")
def api_users():
    """قائمة المسجلين مع الاسم معتّماً (حرف أول ظاهر فقط) ومجموع تبرع كل واحد."""
    users = get_registered_users_with_donations()
    result = [
        {"masked_name": mask_name(u["name"]), "donated": u["donated"]}
        for u in users
    ]
    return jsonify({"users": result})


# ───────────────────────── الويب هوك (استقبال رسائل البوت) ─────────────────────────
@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    update = request.get_json(force=True, silent=True) or {}

    # طلب تأكيد ما قبل الدفع - إلزامي الرد عليه خلال 10 ثواني
    if "pre_checkout_query" in update:
        pcq = update["pre_checkout_query"]
        answer_pre_checkout(pcq["id"], ok=True)
        return jsonify({"ok": True})

    # رسالة نصية عادية (مثل /start) أو إشعار دفع ناجح
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        text = msg.get("text", "") or ""

        sender = msg.get("from", {})
        user_id = sender.get("id")
        if user_id:
            upsert_user(user_id, username=sender.get("username"), first_name=sender.get("first_name"))

        # دفعة ناجحة بنجوم تيليجرام (دعم أو شراء اقتراح)
        if "successful_payment" in msg:
            sp = msg["successful_payment"]
            amount = sp.get("total_amount", 0)
            charge_id = sp.get("telegram_payment_charge_id")
            invoice_payload = sp.get("invoice_payload", "") or ""

            if invoice_payload.startswith("suggest_"):
                # هاي دفعة اقتراح فكرة: منستنى الرسالة الجاية منه وتنحط كمحتوى الاقتراح
                set_pending_action(user_id, "awaiting_suggestion", charge_id)
                send_message(
                    chat_id,
                    "تم الدفع بنجاح 💡\nهلأ اكتبلنا فكرتك أو اقتراحك بالتفصيل برسالة وحدة، وبنوصّلها لفريق التطوير مباشرة 🙏",
                )
            else:
                record_donation(user_id, amount, charge_id)
                send_message(
                    chat_id,
                    f"شكراً إلك من قلبنا 🙏💛\nتم استلام دعمك بقيمة {amount} نجمة ⭐️\nالله يجزيك الخير!",
                )
            return jsonify({"ok": True})

        # لو في اقتراح مدفوع مستني نصّه، ونص الرسالة الحالية مش أمر (يبدأ بـ /)
        pending = get_pending_action(user_id) if user_id else None
        if pending and pending["action"] == "awaiting_suggestion" and text and not text.startswith("/"):
            record_suggestion(user_id, text, pending.get("charge_id"))
            clear_pending_action(user_id)
            send_message(chat_id, "وصلتنا فكرتك وسجّلناها ✅ شكراً إلك على وقتك واهتمامك 🙏💡")
            notify_admin_new_suggestion(user_id, sender.get("username"), text)
            return jsonify({"ok": True})

        if text.startswith("/start"):
            parts = text.split(maxsplit=1)
            start_payload = parts[1].strip() if len(parts) > 1 else ""

            if start_payload == "suggest":
                # جاي من زر "اقترح إضافة" بالموقع: نرسله فاتورة الاقتراح مباشرة
                send_suggestion_invoice(chat_id)
            else:
                site_url = f"https://{os.environ.get('RAILWAY_PUBLIC_DOMAIN', '')}"
                keyboard = {
                    "inline_keyboard": [
                        [{"text": "✅ بدي أتزوج", "callback_data": "want_marry"}],
                        [{"text": "🌐 ادخل", "web_app": {"url": site_url}}],
                        [
                            {"text": "🎁 نقاط اليوم", "callback_data": "claim_points"},
                            {"text": "🏆 الصدارة", "callback_data": "show_leaderboard"},
                        ],
                        [{"text": "⭐ ادعم البوت", "callback_data": "show_donate"}],
                        [{"text": "💡 اقترح إضافة", "callback_data": "propose_idea"}],
                        [{"text": "❌ إلغاء الاشتراك", "callback_data": "unsubscribe"}],
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

        elif text in ("/اقترح", "/suggest"):
            send_suggestion_invoice(chat_id)

        elif text in ("/الاقتراحات", "/suggestions") and str(chat_id) == str(ADMIN_CHAT_ID):
            send_message(chat_id, f"عدد الاقتراحات المستلمة: {get_suggestions_count()} 💡")

        elif text in ("/نقاطي", "/points"):
            send_message(chat_id, f"مجموع نقاطك: {get_user_points(user_id)} ⭐")

        elif text in ("/الصدارة", "/leaderboard"):
            send_message(chat_id, build_leaderboard_text())

        elif text in ("/انسحب", "/unsubscribe"):
            if is_registered(user_id):
                send_message(
                    chat_id,
                    "متأكد بدك تنسحب من قائمة المسجلين للزواج؟",
                    unsubscribe_confirm_keyboard(),
                )
            else:
                send_message(chat_id, "أنت مش مسجل أصلاً بالقائمة 🙂")

    # ضغطة على الزر (Callback Query)
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
                send_donation_invoice(chat_id, amount)
            else:
                answer_callback(callback_id, "قيمة غير صالحة 🙏")

        elif data_key == "propose_idea":
            answer_callback(callback_id, f"جاري تجهيز فاتورة {SUGGESTION_PRICE} نجمة ⭐")
            send_suggestion_invoice(chat_id)

        elif data_key == "claim_points":
            success, total = claim_daily_points(user_id)
            if success:
                answer_callback(callback_id, f"🎁 +{DAILY_POINTS} نقطة!")
                send_message(
                    chat_id,
                    f"أخدت {DAILY_POINTS} نقطة اليوم 🎁\nمجموع نقاطك الآن: {total} ⭐\nرجعلنا بكرا تاخد نقاط كمان!",
                )
            else:
                answer_callback(
                    callback_id,
                    "أخدت نقاط اليوم مسبقاً، رجعلنا بكرا 🙏",
                    show_alert=True,
                )

        elif data_key == "show_leaderboard":
            answer_callback(callback_id, "")
            send_message(chat_id, build_leaderboard_text())

        elif data_key == "unsubscribe":
            answer_callback(callback_id, "")
            if is_registered(user_id):
                send_message(
                    chat_id,
                    "متأكد بدك تنسحب من قائمة المسجلين للزواج؟",
                    unsubscribe_confirm_keyboard(),
                )
            else:
                send_message(chat_id, "أنت مش مسجل أصلاً بالقائمة 🙂")

        elif data_key == "confirm_unsubscribe":
            removed = unregister_user(user_id)
            if removed:
                answer_callback(callback_id, "تم الانسحاب ✅")
                send_message(
                    chat_id,
                    f"تم حذفك من قائمة المسجلين 👋\nعدد الراغبين بالزواج الآن: {get_count()} 💍",
                )
            else:
                answer_callback(callback_id, "ما كنت مسجل أصلاً 🙂")

        elif data_key == "cancel_unsubscribe":
            answer_callback(callback_id, "تم التراجع 👍")

    return jsonify({"ok": True})


# ───────────────────────── تفعيل الويب هوك تلقائياً ─────────────────────────
def set_webhook():
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if not domain or not BOT_TOKEN:
        print("BOT_TOKEN أو RAILWAY_PUBLIC_DOMAIN غير موجودين، لن يتم ضبط الويب هوك تلقائياً.")
        return
    url = f"https://{domain}/webhook/{BOT_TOKEN}"
    try:
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
