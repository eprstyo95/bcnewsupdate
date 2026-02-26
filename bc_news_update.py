#!/usr/bin/env python3
import os
import re
import hashlib
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

# Telegram batching
MAX_ITEMS_PER_BATCH = 8
SEND_HEARTBEAT = True  # set False kalau mau silent saat tidak ada update

WIB = timezone(timedelta(hours=7))

# =========================
# ENV VARS (GitHub Secrets)
# =========================
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")  # allow RSS-only
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# =========================
# HTTP SESSION + RETRY
# =========================
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (bc-news-bot)"})
    return s

def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: int = 20,
    max_tries: int = 3,
    backoff_s: float = 1.5,
    **kwargs
):
    last_err = None
    for i in range(max_tries):
        try:
            return session.request(method, url, timeout=timeout, **kwargs)
        except Exception as e:
            last_err = e
            if i < max_tries - 1:
                try:
                    import time
                    time.sleep(backoff_s * (2 ** i))
                except Exception:
                    pass
    raise last_err

# =========================
# DATABASE (SEEN)
# =========================
def init_db(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            fingerprint TEXT PRIMARY KEY,
            url TEXT,
            title TEXT,
            first_seen_utc TEXT
        )
    """)
    con.commit()

def is_seen(con: sqlite3.Connection, fingerprint: str) -> bool:
    cur = con.cursor()
    cur.execute("SELECT 1 FROM seen WHERE fingerprint = ?", (fingerprint,))
    return cur.fetchone() is not None

def mark_seen(con: sqlite3.Connection, fingerprint: str, url: str, title: str):
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO seen (fingerprint, url, title, first_seen_utc) VALUES (?, ?, ?, ?)",
        (fingerprint, url, title, datetime.now(timezone.utc).isoformat())
    )
    con.commit()

# =========================
# HELPERS
# =========================
TRACKING_PARAMS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","fbclid","gclid"}

def norm_url(u: str) -> str:
    """Normalize URL: drop fragment + common tracking params."""
    if not u:
        return ""
    u = u.split("#", 1)[0].strip()
    parts = urlsplit(u)
    q = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in TRACKING_PARAMS]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), ""))

def normalize_title(t: str) -> str:
    if not t:
        return ""
    t = t.strip().lower()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[“”\"'’]+", "", t)
    return t

def make_fingerprint(url: str, title: str) -> str:
    base = f"{norm_url(url)}|{normalize_title(title)}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

def resolve_final_url(session: requests.Session, u: str) -> str:
    """Resolve redirects (HEAD first, GET stream fallback)."""
    if not u:
        return ""
    u = norm_url(u)

    try:
        r = request_with_retry(session, "HEAD", u, timeout=12, allow_redirects=True)
        if r is not None and getattr(r, "url", None):
            return norm_url(r.url)
    except Exception:
        pass

    try:
        r = request_with_retry(session, "GET", u, timeout=15, allow_redirects=True, stream=True)
        if r is not None and getattr(r, "url", None):
            return norm_url(r.url)
    except Exception:
        pass

    return u

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

def make_hashtags(title: str, url: str = "") -> list[str]:
    t = (title or "").lower()
    u = (url or "").lower()

    TAGS = [
        (["djbc", "bea cukai", "customs"], "#DJBC"),
        (["kemenkeu", "kementerian keuangan", "menkeu", "sri mulyani"], "#Kemenkeu"),
        (["impor", "import"], "#Impor"),
        (["ekspor", "export"], "#Ekspor"),
        (["rokok", "tembakau", "cukai", "sigaret"], "#Cukai"),
        (["narkoba", "drug", "meth", "sabu", "kokain"], "#Narkotika"),
        (["penyelundupan", "smuggling", "ilegal"], "#Penyelundupan"),
        (["plb", "kawasan berikat", "kb", "kite"], "#Fasilitas"),
        (["pelabuhan", "tanjung priok", "soekarno hatta", "bandara"], "#Logistik"),
        (["tarif", "bea masuk", "pajak", "ppn", "pnbp"], "#Tarif"),
        (["wco", "wto", "asean", "fta", "ska", "origin"], "#Perdagangan"),
        (["penindakan", "operasi", "sitaan", "gagalkan"], "#Penindakan"),
        (["aturan", "pmk", "peraturan", "regulasi"], "#Regulasi"),
    ]

    out = []
    for keys, tag in TAGS:
        if any(k in t or k in u for k in keys):
            out.append(tag)

    if not out:
        out = ["#BCNews"]

    return out[:5]

def short_display_url(u: str, max_len: int = 60) -> str:
    """
    Visual short link only (domain + truncated path).
    Full URL tetap dikirim di baris berikutnya agar tetap clickable.
    """
    if not u:
        return ""

    parts = urlsplit(u)
    display = f"{parts.netloc}{parts.path}"
    if parts.query:
        display += "?"
    if len(display) > max_len:
        display = display[:max_len - 1] + "…"
    return display

# =========================
# TELEGRAM
# =========================
def telegram_send(session: requests.Session, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = request_with_retry(
        session,
        "POST",
        url,
        timeout=25,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": False},
    )
    print("Telegram:", r.status_code, (r.text or "")[:140])

def chunk_text(text: str, limit: int = 3500):
    """Telegram limit 4096; keep buffer."""
    if len(text) <= limit:
        return [text]
    chunks = []
    cur = ""
    for line in text.splitlines(True):
        if len(cur) + len(line) > limit:
            chunks.append(cur)
            cur = ""
        cur += line
    if cur:
        chunks.append(cur)
    return chunks

def send_updates_batched(session: requests.Session, updates: list[dict]):
    if not updates:
        return
    batch = []
    for it in updates:
        batch.append(it)
        if len(batch) >= MAX_ITEMS_PER_BATCH:
            _send_one_batch(session, batch)
            batch = []
    if batch:
        _send_one_batch(session, batch)

def _send_one_batch(session: requests.Session, batch: list[dict]):
    lines = ["🛃 BC News Update (latest)"]

    for it in batch:
        pub = it.get("published_utc")
        title = (it.get("title") or "").strip()
        url = (it.get("url") or "").strip()
        src = (it.get("source") or "-").strip()

        tags = " ".join(make_hashtags(title, url))
        short_label = short_display_url(url)

        lines.append("")
        lines.append(f"📰 {title}")
        lines.append(f"🕒 {fmt_wib(pub)}")
        lines.append(f"📌 {src}")
        lines.append(f"🏷️ {tags}")
        lines.append(f"🔗 {short_label}")
        lines.append(url)

    text = "\n".join(lines)
    for part in chunk_text(text):
        telegram_send(session, part)

# =========================
# GOOGLE NEWS RSS
# =========================
def fetch_google_news_rss(session: requests.Session, query: str):
    rss_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=id&gl=ID&ceid=ID:id"
    feed = feedparser.parse(rss_url)

    out = []
    for entry in feed.entries[:GOOGLE_RSS_SIZE]:
        pub = entry_published_utc(entry)
        link = entry.get("link") or ""
        title = (entry.get("title") or "").strip()
        out.append({
            "source": "GoogleNews",
            "title": title,
            "url": resolve_final_url(session, link),
            "published_utc": pub,
        })
    return out

# =========================
# NEWSAPI
# =========================
def fetch_newsapi(session: requests.Session, query: str, cutoff_utc: datetime):
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
        "from": cutoff_utc.isoformat(),
    }
    if NEWSAPI_LANGUAGE:
        params["language"] = NEWSAPI_LANGUAGE

    r = request_with_retry(session, "GET", url, params=params, timeout=25)
    try:
        data = r.json()
    except Exception:
        print("⚠️ NewsAPI non-JSON:", (r.text or "")[:200])
        return []

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
    session = build_session()
    con = sqlite3.connect(DB_FILE)

    try:
        init_db(con)

        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(hours=MAX_AGE_HOURS)

        items = []
        items += fetch_google_news_rss(session, QUERY_RSS)
        items += fetch_newsapi(session, QUERY_NEWSAPI, cutoff)

        # Deduplicate within run by fingerprint (url+title)
        by_fp: dict[str, dict] = {}
        for it in items:
            if not it.get("url") and not it.get("title"):
                continue
            fp = make_fingerprint(it.get("url", ""), it.get("title", ""))
            if fp not in by_fp:
                by_fp[fp] = it
            else:
                old = by_fp[fp]
                old_pub = old.get("published_utc")
                new_pub = it.get("published_utc")
                if old_pub is None and new_pub is not None:
                    by_fp[fp] = it
                elif old_pub is not None and new_pub is not None and new_pub > old_pub:
                    by_fp[fp] = it

        items = list(by_fp.values())

        # Sort latest first
        items = sorted(
            items,
            key=lambda x: x.get("published_utc") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True
        )

        new_items = []
        too_old = 0
        no_date = 0
        seen_skip = 0

        for it in items:
            pub = it.get("published_utc")
            if pub is None:
                no_date += 1
                continue
            if pub < cutoff:
                too_old += 1
                continue

            url = it.get("url") or ""
            title = it.get("title") or ""
            fp = make_fingerprint(url, title)

            if is_seen(con, fp):
                seen_skip += 1
                continue

            mark_seen(con, fp, url, title)
            new_items.append(it)

        # Send updates batched
        send_updates_batched(session, new_items)

        # Heartbeat
        if SEND_HEARTBEAT:
            telegram_send(
                session,
                f"✅ BC monitor OK. New: {len(new_items)}. "
                f"Skipped seen: {seen_skip}. Skipped old: {too_old}. "
                f"No-date skipped: {no_date}. Fetched(after dedupe): {len(items)}. Window: {MAX_AGE_HOURS}h."
            )

        print(
            f"Done. New={len(new_items)}, seen_skipped={seen_skip}, "
            f"old_skipped={too_old}, no_date_skipped={no_date}, fetched={len(items)}"
        )

    finally:
        try:
            con.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
