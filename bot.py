import requests

BOT_TOKEN = "8667627503:AAF3g679L4g5IxIS6s8Dhf7QM-tWug8_kFk"

url = f"https://api.telegram.org/bot{BOT_TOKEN}/getMe"
response = requests.get(url)

print("Status code:", response.status_code)
print("Response:", response.json())

if response.json().get("ok"):
    bot_info = response.json()["result"]
    print(f"\n✅ التوكن شغّال! اسم البوت: {bot_info['first_name']} (@{bot_info['username']})")
else:
    print("\n❌ التوكن غير صحيح أو ملغي.")
