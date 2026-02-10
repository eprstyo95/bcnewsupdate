#!/usr/bin/env python3
import os
import sqlite3
import requests
import feedparser
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, urlsplit, urlunsplit, parse_qsl, urlencode

# =========================
# SETTINGS
# =========================
DB_FILE = "seen.sqlite"

# Query utama (RSS + NewsAPI)
# (3) Tambahin operator waktu Google News: when:24h (sesuaikan dengan MAX_AGE_HOURS kalau mau)
QUERY_RSS = 'bea cukai OR DJBC OR Kemenkeu OR "Kementerian Keuangan" when:24h'
QUERY_NEWSAPI = '"bea cukai" OR DJBC OR Kemenkeu OR "Kementerian Keuangan"'

# Berapa jam ke belakang yang dianggap "latest"
MAX_AGE_HOURS = 24

# Batas jumlah item yang ditarik per run
GOOGLE_RSS_SIZE = 30
NEWSAPI_PAGE_SIZE = 20

# NewsAPI tuning (biar nggak banyak press-release)
NEWSAPI_LANGUAGE = "id"  # set None kalau mau global
NEWSAPI_EXCLUDE_DOMAINS = "globenewswire.com,prnewswire.com,businesswire.com"

WIB = timezone(timedelta(hours=7))

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
        (url, datetime.now(timezone.utc).isoformat())
    )
    con.commit()
    con.close()

# =========================
# HELPERS
# =========================
TRACKING_PARAMS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","fbclid","gclid"}

def norm_url(u: str) -> str:
    """Normalize URL: drop fragment + common tracking params (kept minimal & safe)."""
    if not u:
        return ""
    u = u.split("#", 1)[0].strip()
    parts = urlsplit(u)
    q = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in TRACKING_PARAMS]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), ""))

# (2) Google News RSS URL -> resolve final URL (follow redirects)
def resolve_final_url(u: str) -> str:
    if not u:
        return ""
    u = norm_url(u)
    try:
        r = requests.get(
            u,
            timeout=15,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        return norm_url(r.url)
    except Exception:
        return u

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": False},
        timeout=20,
    )
    print("Telegram:", r.status_code, r.text[:140])

def fmt_wib(dt_utc: datetime | None) -> str:
    if not dt_utc:
        return "Unknown"
    return dt_utc.astimezone(WIB).strftime("%Y-%m-%d %H:%M WIB")

def parse_newsapi_datetime(s: str | None) -> datetime | None:
    # Example: "2026-02-10T07:12:00Z"
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def entry_published_utc(entry) -> datetime | None:
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    if not t:
        return None
    return datetime(*t[:6], tzinfo=timezone.utc)

# =========================
# GOOGLE NEWS RSS
# =========================
def fetch_google_news_rss(query: str):
    rss_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=id&gl=ID&ceid=ID:id"
    feed = feedparser.parse(rss_url)

    out = []
    for entry in feed.entries[:GOOGLE_RSS_SIZE]:
        pub = entry_published_utc(entry)
        link = entry.get("link") or ""
        out.append({
            "source": "GoogleNews",
            "title": (entry.get("title") or "").strip(),
            "url": resolve_final_url(link),  # (2) resolve final URL
            "published_utc": pub,
        })
    return out

# =========================
# NEWSAPI
# =========================
def fetch_newsapi(query: str, cutoff_utc: datetime):
    if not NEWSAPI_KEY:
        return []

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "searchIn": "title,description",
        "sortBy": "publishedAt",
        "pageSize": NEWSAPI_PAGE_SIZE,
        "apiKey": NEWSAPI_KEY,
        "excludeDomains": NEWSAPI_EXCLUDE_DOMAINS,
        # (4) parameter from newsapi: batasi rentang sesuai cutoff
        "from": cutoff_utc.isoformat(),
    }
    if NEWSAPI_LANGUAGE:
        params["language"] = NEWSAPI_LANGUAGE

    r = requests.get(url, params=params, timeout=25)
    data = r.json()

    if data.get("status") != "ok":
        print("⚠️ NewsAPI error:", data)
        return []

    out = []
    for a in data.get("articles", []):
        pub = parse_newsapi_datetime(a.get("publishedAt"))
        out.append({
            "source": f"NewsAPI:{(a.get('source', {}) or {}).get('name','')}".strip(),
            "title": (a.get("title") or "").strip(),
            "url": norm_url(a.get("url") or ""),
            "published_utc": pub,
        })
    return out

# =========================
# MAIN
# =========================
def main():
    init_db()

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=MAX_AGE_HOURS)

    items = []
    items += fetch_google_news_rss(QUERY_RSS)
    items += fetch_newsapi(QUERY_NEWSAPI, cutoff)  # (4) pass cutoff for NewsAPI from=

    # Deduplicate within this run by URL
    by_url = {}
    for it in items:
        if it["url"]:
            if it["url"] not in by_url:
                by_url[it["url"]] = it
            else:
                if (by_url[it["url"]].get("published_utc") is None) and (it.get("published_utc") is not None):
                    by_url[it["url"]] = it

    items = list(by_url.values())

    # (1) Urutan berita: sort published_utc desc (latest dulu)
    items = sorted(
        items,
        key=lambda x: x.get("published_utc") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True
    )

    new_count = 0
    too_old = 0
    no_date = 0

    for it in items:
        url = it["url"]
        if not url:
            continue

        pub = it.get("published_utc")
        if pub is None:
            no_date += 1
            continue

        if pub < cutoff:
            too_old += 1
            continue

        if is_seen(url):
            continue

        mark_seen(url)
        new_count += 1

        msg = (
            "🛃 BC News Update\n"
            f"📰 {it['title']}\n"
            f"🕒 Published: {fmt_wib(pub)}\n"
            f"📌 {it['source']}\n"
            f"🔗 {url}"
        )
        send_telegram(msg)

    # Confirmation (heartbeat)
    send_telegram(
        f"✅ BC monitor OK. New: {new_count}. Skipped old: {too_old}. "
        f"No-date skipped: {no_date}. Fetched: {len(items)}. Window: {MAX_AGE_HOURS}h."
    )

    print(f"Done. New={new_count}, old_skipped={too_old}, no_date_skipped={no_date}, fetched={len(items)}")

if __name__ == "__main__":
    main()

