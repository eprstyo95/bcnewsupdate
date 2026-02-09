#!/usr/bin/env python3
import os
import requests

# This script ONLY sends a ping to Telegram.
# Purpose: confirm GitHub Actions secrets + Telegram delivery works.

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True},
        timeout=20,
    )
    print("Telegram:", r.status_code, r.text)

def main():
    send_telegram("✅ GitHub Actions ping: Telegram config OK")

if __name__ == "__main__":
    main()
