#!/usr/bin/env python3
import os
import sqlite3
import requests
import feedparser
from datetime import datetime
from urllib.parse import quote

# =========================
# SETTINGS
# =========================
GOOGLE_RSS_SIZE = 10
DB_FILE = "seen.sqlite"

QUERY = 'bea cukai OR DJBC OR Kemenkeu OR "Kementerian Keuangan"'

# =========================
# ENV VARS (GitHub Secrets)
# =========================
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# =========================
# DATABASE (SEEN)
# =========================
def init_db():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            url TEXT PRIMARY KEY,
            first_seen_utc TEXT
        )
    """)
    con.commit()
    con.close()

def is_seen(url: str) -> bool:
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM seen WHERE url = ?", (url,))
    row = cur.fetchone()
    con.close()
    return row is not None

def mark_seen(url: str):
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO seen (url, first_seen_utc) VALUES (?, ?)",
        (url, datetime.utcnow().isoformat())
    )
    con.commit()
    con.close()

# =========================
# HELPERS
# =========================
def norm_url(u: str) -> str:
    if not u:
        return ""
    return u.split("#", 1)[0].strip()

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": False},
        timeout=20,
    )
    print("Telegram:", r.status_code)

# =========================
# GOOGLE NEWS RSS
# =========================
def fetch_google_news_rss(query: str):
    rss_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=id&gl=ID&ceid=ID:id"
    feed = feedparser.parse(rss_url)

    out = []
    for entry in feed.entries[:GOOGLE_RSS_SIZE]:
        out.append({
            "source": "GoogleNews",
            "title": (entry.get("title") or "").strip(),
            "url": norm_url(entry.get("link") or ""),
        })
    return out

# =========================
# MAIN
# =========================
def main():
    init_db()

    items = fetch_google_news_rss(QUERY)

    new_count = 0
    for it in items:
        url = it["url"]
        if not url:
            continue
        if is_seen(url):
            continue

        mark_seen(url)
        new_count += 1

        msg = (
            "🛃 BC News Update\n"
            f"📰 {it['title']}\n"
            f"📌 {it['source']}\n"
            f"🔗 {url}"
        )
        send_telegram(msg)

    print(f"Done. New alerts: {new_count}")

if __name__ == "__main__":
    main()
