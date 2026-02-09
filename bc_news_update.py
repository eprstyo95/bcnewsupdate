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
NEWSAPI_PAGE_SIZE = 10
GOOGLE_RSS_SIZE = 10

NEWSAPI_LANGUAGE = "id"  # set to None if you want global sources
NEWSAPI_EXCLUDE_DOMAINS = "globenewswire.com,prnewswire.com,businesswire.com"

QUERY_PRIMARY = '"bea cukai" OR DJBC OR Kemenkeu OR "Kementerian Keuangan"'
QUERY_LAST_RESORT = "bea cukai"

DB_FILE = "seen.sqlite"

# =========================
# ENV VARS (GitHub Secrets)
# =========================
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")  # allow RSS-only
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
    print("Telegram:", r.status_code, r.text[:180])

# =========================
# NEWSAPI
# =========================
def fetch_newsapi(query: str, use_language: bool):
    if not NEWSAPI_KEY:
        return []

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "sortBy": "publishedAt",
        "pageSize": NEWSAPI_PAGE_SIZE,
        "apiKey": NEWSAPI_KEY,
        "excludeDomains": NEWSAPI_EXCLUDE_DOMAINS,
        "searchIn": "title,description",
    }
    if use_language and NEWSAPI_LANGUAGE:
        params["language"] = NEWSAPI_LANGUAGE

    r = requests.get(url, params=params, timeout=25)
    data = r.json()

    if data.get("status") != "ok":
        print("⚠️ NewsAPI error:", data)
        return []

    out = []
    for a in data.get("articles", []):
        out.append({
            "source": f"NewsAPI:{(a.get('source', {}) or {}).get('name','')}".strip(),
            "title": (a.get("title") or "").strip(),
            "url": norm_url(a.get("url") or ""),
        })
    return out

def fetch_newsapi_with_fallback():
    attempts = [
        (QUERY_PRIMARY, True),
        (QUERY_PRIMARY, False),
        (QUERY_LAST_RESORT, True),
        (QUERY_LAST_RESORT, False),
    ]
    for q, use_lang in attempts:
        items = fetch_newsapi(q, use_language=use_lang)
        if items:
            return items
    return []

# =========================
# GOOGLE NEWS RSS
# =========================
def fetch_google_news_rss(query: str):
    rss_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=id&gl=ID&ceid=ID:id"
    feed = feedparser.parse(rss_url)

    out = []
    for entry in feed.entries[:GOOGLE_RSS_SIZE]:
        out.append({
            "source": "GoogleNewsRSS",
            "title": (entry.get("title") or "").strip(),
            "url": norm_url(entry.get("link") or ""),
        })
    return out

# =========================
# MAIN
# =========================
def main():
    init_db()

    items = []
    items += fetch_newsapi_with_fallback()
    items += fetch_google_news_rss("bea cukai OR DJBC OR Kemenkeu")

    # Deduplicate within this run (same URL from both sources)
    by_url = {}
    for it in items:
        if it["url"]:
            by_url[it["url"]] = it
    items = list(by_url.values())

    new_count = 0
    for it in items:
        url = it["url"]
        if not url:
            continue

        if is_seen(url):
            continue  # already sent before (seen)

        mark_seen(url)
        new_count += 1

        msg = (
            "🛃 BC News Update\n"
            f"📰 {it['title']}\n"
            f"📌 {it['source']}\n"
            f"🔗 {url}"
        )
        send_telegram(msg)

    print(f"Done. New alerts sent: {new_count}. Total fetched: {len(items)}")

if __name__ == "__main__":
    main()

