#!/usr/bin/env python3
import os
import requests
import feedparser
from urllib.parse import quote

# =========================
# SETTINGS
# =========================
NEWSAPI_PAGE_SIZE = 5
GOOGLE_RSS_SIZE = 5

NEWSAPI_LANGUAGE = "id"  # set to None if you want global sources
NEWSAPI_EXCLUDE_DOMAINS = "globenewswire.com,prnewswire.com,businesswire.com"

# Queries (keep these simple & reliable)
QUERY_PRIMARY = '"bea cukai" OR DJBC OR Kemenkeu OR "Kementerian Keuangan"'
QUERY_LAST_RESORT = "bea cukai"

# =========================
# ENV VARS
# =========================
NEWSAPI_KEY = os.environ["NEWSAPI_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# =========================
# TELEGRAM
# =========================
def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": False},
        timeout=20,
    )
    print("Telegram:", r.status_code)

# =========================
# NEWSAPI
# =========================
def fetch_newsapi(query: str, use_language: bool):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "sortBy": "publishedAt",
        "pageSize": NEWSAPI_PAGE_SIZE,
        "apiKey": NEWSAPI_KEY,
        "excludeDomains": NEWSAPI_EXCLUDE_DOMAINS,
        "searchIn": "title,description",  # less strict than title-only
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
            "title": (a.get("title") or "").strip(),
            "source": (a.get("source", {}) or {}).get("name", "").strip(),
            "url": (a.get("url") or "").strip(),
        })
    return out

def fetch_newsapi_with_fallback():
    # 1) primary query with language
    items = fetch_newsapi(QUERY_PRIMARY, use_language=True)
    if items:
        return items, f'primary(id): {QUERY_PRIMARY}'

    # 2) primary query without language (global)
    items = fetch_newsapi(QUERY_PRIMARY, use_language=False)
    if items:
        return items, f'primary(no-lang): {QUERY_PRIMARY}'

    # 3) last resort simple query (with language)
    items = fetch_newsapi(QUERY_LAST_RESORT, use_language=True)
    if items:
        return items, f'lastresort(id): {QUERY_LAST_RESORT}'

    # 4) last resort simple query without language
    items = fetch_newsapi(QUERY_LAST_RESORT, use_language=False)
    return items, f'lastresort(no-lang): {QUERY_LAST_RESORT}'

# =========================
# GOOGLE NEWS RSS (2nd source)
# =========================
def fetch_google_news_rss(query: str):
    # Google News RSS search
    rss_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=id&gl=ID&ceid=ID:id"
    feed = feedparser.parse(rss_url)

    out = []
    for entry in feed.entries[:GOOGLE_RSS_SIZE]:
        out.append({
            "title": (entry.get("title") or "").strip(),
            "source": "GoogleNewsRSS",
            "url": (entry.get("link") or "").strip(),
        })
    return out, rss_url

# =========================
# MAIN
# =========================
def main():
    # ---- NEWSAPI ----
    news_items, mode = fetch_newsapi_with_fallback()
    if not news_items:
        print("No NewsAPI articles found (after fallback).")
    else:
        print("✅ NewsAPI mode used:", mode)
        for it in news_items:
            msg = (
                "🛃 Bea Cukai / Kemenkeu (NewsAPI)\n"
                f"📰 {it['title']}\n"
                f"📌 {it['source']}\n"
                f"🔗 {it['url']}"
            )
            send_telegram(msg)

    # ---- GOOGLE NEWS RSS ----
    rss_items, rss_url = fetch_google_news_rss("bea cukai OR DJBC OR Kemenkeu")
    if not rss_items:
        print("No Google News RSS items found.")
    else:
        print("✅ Google News RSS OK:", rss_url)
        for it in rss_items:
            msg = (
                "🗞️ Bea Cukai / Kemenkeu (Google News RSS)\n"
                f"📰 {it['title']}\n"
                f"🔗 {it['url']}"
            )
            send_telegram(msg)

if __name__ == "__main__":
    main()

