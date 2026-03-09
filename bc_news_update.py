#!/usr/bin/env python3
"""
BC News Monitor — improved version
  • Local keyword-based sentiment analysis (Indonesian + English, zero cost)
  • Richer Telegram formatting with inline-keyboard buttons
  • Article snippet / description included
  • Sentiment emoji indicator per article
"""

import os
import re
import html
import hashlib
import sqlite3
import requests
import feedparser
import json
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, urlsplit, urlunsplit, parse_qsl, urlencode

# =========================
# SETTINGS
# =========================
DB_FILE = "seen.sqlite"

QUERY_RSS = 'bea cukai OR DJBC OR Kemenkeu OR "Kementerian Keuangan" when:24h'
QUERY_NEWSAPI = '(bea cukai OR DJBC OR Kemenkeu OR "Kementerian Keuangan")'

MAX_AGE_HOURS = 24

GOOGLE_RSS_SIZE = 30
NEWSAPI_PAGE_SIZE = 20

NEWSAPI_LANGUAGE = "id"  # set None kalau mau global
NEWSAPI_EXCLUDE_DOMAINS = "globenewswire.com,prnewswire.com,businesswire.com"

MAX_ITEMS_PER_BATCH = 1
SEND_HEARTBEAT = True

DEBUG_NEWSAPI = False  # set True kalau mau debug NewsAPI

# Snippet / description settings
INCLUDE_SNIPPET = True       # include article snippet in Telegram message
SNIPPET_MAX_CHARS = 150      # max chars for snippet

WIB = timezone(timedelta(hours=7))

# =========================
# ENV VARS (GitHub Secrets)
# =========================
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")  # allow RSS-only
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# =========================
# SENTIMENT ANALYSIS (local, free)
# =========================

# Indonesian + English keywords for sentiment detection
# Tuned for Bea Cukai / customs / government news context
SENTIMENT_POSITIVE_KW = [
    # Pencapaian & penerimaan
    "berhasil", "sukses", "prestasi", "penghargaan", "apresiasi",
    "meningkat", "pertumbuhan", "positif", "optimis", "inovasi",
    "kemudahan", "fasilitasi", "percepat", "reformasi", "terobosan",
    "efisien", "efektif", "capaian", "raih", "rekor",
    "penerimaan naik", "penerimaan melampaui", "surplus", "peningkatan",
    "kolaborasi", "sinergi", "dukungan", "layanan prima", "mudahkan",
    "zona integritas", "wilayah bebas korupsi", "wbk", "wbbm",
    "digitalisasi", "modernisasi", "transformasi",
    # Penegakan hukum / enforcement (= prestasi BC)
    "penindakan", "tindakan tegas", "tindak tegas",
    "sitaan", "sita", "disita", "menyita",
    "gagalkan", "berhasil gagalkan", "berhasil ungkap",
    "amankan", "diamankan", "berhasil amankan",
    "tangkap", "tertangkap", "ditangkap", "ditahan",
    "penyelundupan", "selundupkan", "smuggling", "barang selundupan",
    "narkoba", "narkotika", "sabu", "kokain", "ganja",
    "rokok ilegal", "miras ilegal",
    "selamatkan uang negara", "lindungi masyarakat",
    # English
    "success", "achievement", "growth", "innovation", "efficient",
    "record", "surplus", "improvement", "collaboration",
    "seized", "arrested", "intercepted", "crackdown",
]

SENTIMENT_NEGATIVE_KW = [
    # Korupsi & internal (= berita buruk BC)
    "korupsi", "suap", "gratifikasi", "pungli", "pungutan liar",
    "oknum", "oknum bea cukai", "penyalahgunaan", "penyalahgunaan wewenang",
    "fraud", "pemalsuan", "palsu",
    "dugaan korupsi", "kasus korupsi", "tersangka korupsi",
    # Kegagalan & masalah
    "gagal", "kegagalan", "masalah", "keluhan", "hambatan",
    "penurunan", "defisit", "rugi", "kerugian negara",
    "bocor", "kebocoran", "penyimpangan",
    "terlambat", "tertunda", "lambat",
    # Pelanggaran regulasi (oleh pihak internal/petugas)
    "pelanggaran kode etik", "pelanggaran disiplin",
    "diduga", "dugaan",
    # Ancaman & krisis
    "ancaman", "bahaya", "krisis", "darurat",
    "meresahkan", "merugikan", "kontrovers", "polemik",
    # English
    "corruption", "bribery", "fraud", "decline", "deficit",
    "failure", "complaint", "scandal",
]

SENTIMENT_NEUTRAL_KW = [
    # Words that strongly indicate neutral/informational content
    "sosialisasi", "edukasi", "kunjungan", "rapat", "koordinasi",
    "peraturan baru", "pmk", "ketentuan", "audiensi", "mou",
    "workshop", "seminar", "bimtek", "pelatihan",
]


def analyze_sentiment(title: str, description: str = "") -> dict:
    """
    Analyze sentiment of an article based on title + description.
    Returns: {"label": "Positif"|"Negatif"|"Netral", "emoji": str, "score": float}
    Score: -1.0 (most negative) to +1.0 (most positive)
    """
    text = f"{title} {description}".lower().strip()

    if not text:
        return {"label": "Netral", "emoji": "⚪", "score": 0.0}

    pos_hits = sum(1 for kw in SENTIMENT_POSITIVE_KW if kw in text)
    neg_hits = sum(1 for kw in SENTIMENT_NEGATIVE_KW if kw in text)
    neu_hits = sum(1 for kw in SENTIMENT_NEUTRAL_KW if kw in text)

    total = pos_hits + neg_hits + neu_hits
    if total == 0:
        return {"label": "Netral", "emoji": "⚪", "score": 0.0}

    # If strong neutral signal and no dominant pos/neg, mark neutral
    if neu_hits > 0 and abs(pos_hits - neg_hits) <= 1:
        return {"label": "Netral", "emoji": "⚪", "score": 0.0}

    # Calculate score
    score = (pos_hits - neg_hits) / total
    score = max(-1.0, min(1.0, score))

    if score > 0.15:
        return {"label": "Positif", "emoji": "🟢", "score": round(score, 2)}
    elif score < -0.15:
        return {"label": "Negatif", "emoji": "🔴", "score": round(score, 2)}
    else:
        return {"label": "Netral", "emoji": "⚪", "score": round(score, 2)}


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
# DATABASE (SEEN) + MIGRATION
# =========================
TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid"}


def norm_url(u: str) -> str:
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
    t = re.sub(r"[""\"'']+", "", t)
    return t


def make_fingerprint(url: str, title: str) -> str:
    base = f"{norm_url(url)}|{normalize_title(title)}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def init_db(con: sqlite3.Connection):
    """
    Init + auto-migrate schema:
    - OLD schema: seen(url TEXT PRIMARY KEY, first_seen_utc TEXT)
    - NEW schema: seen(fingerprint TEXT PRIMARY KEY, url TEXT, title TEXT, first_seen_utc TEXT)
    """
    cur = con.cursor()

    # Check if table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='seen'")
    exists = cur.fetchone() is not None

    if not exists:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seen (
                fingerprint TEXT PRIMARY KEY,
                url TEXT,
                title TEXT,
                first_seen_utc TEXT
            )
        """)
        con.commit()
        return

    # Inspect columns
    cur.execute("PRAGMA table_info(seen)")
    cols = [row[1] for row in cur.fetchall()]

    if "fingerprint" in cols:
        return

    # Old schema -> migrate
    print("🔁 Migrating seen.sqlite schema: old(url) -> new(fingerprint,url,title,first_seen_utc)")
    cur.execute("ALTER TABLE seen RENAME TO seen_old")

    cur.execute("""
        CREATE TABLE seen (
            fingerprint TEXT PRIMARY KEY,
            url TEXT,
            title TEXT,
            first_seen_utc TEXT
        )
    """)

    cur.execute("SELECT url, first_seen_utc FROM seen_old")
    rows = cur.fetchall()

    for url, first_seen_utc in rows:
        fp = make_fingerprint(url or "", "")
        cur.execute(
            "INSERT OR IGNORE INTO seen (fingerprint, url, title, first_seen_utc) VALUES (?, ?, ?, ?)",
            (fp, url, "", first_seen_utc),
        )

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
# MORE HELPERS
# =========================
def resolve_final_url(session: requests.Session, u: str) -> str:
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


def fmt_wib(dt_utc):
    if not dt_utc:
        return "Unknown"
    return dt_utc.astimezone(WIB).strftime("%Y-%m-%d %H:%M WIB")


def parse_newsapi_datetime(s: str):
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def entry_published_utc(entry):
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    if not t:
        return None
    return datetime(*t[:6], tzinfo=timezone.utc)


def clean_html_description(raw: str, max_chars: int = SNIPPET_MAX_CHARS) -> str:
    """Strip HTML tags from RSS description and truncate."""
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", "", raw)
    text = html.unescape(text).strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) > max_chars:
        text = text[:max_chars].rsplit(" ", 1)[0] + "…"
    return text


def make_hashtags(title: str, url: str = ""):
    t = (title or "").lower()
    u = (url or "").lower()

    TAGS = [
        (["djbc", "bea cukai", "customs"], "#DJBC"),
        (["kemenkeu", "kementerian keuangan", "menkeu", "sri mulyani"], "#Kemenkeu"),
        (["kanwil", "kppbc", "kantor bea cukai"], "#KantorBC"),

        (["purbaya yudhi", "purbaya yudhi sadewa", "purbaya"], "#Purbaya"),
        (["marunda", "kawasan marunda", "pelabuhan marunda"], "#Marunda"),

        (["impor", "import"], "#Impor"),
        (["ekspor", "export"], "#Ekspor"),
        (["transit"], "#Transit"),
        (["re-ekspor", "reekspor"], "#ReEkspor"),

        (["plb", "pusat logistik berikat"], "#PLB"),
        (["kawasan berikat", "kb"], "#KawasanBerikat"),
        (["kite", "ikm"], "#KITE"),
        (["gudang berikat"], "#GudangBerikat"),

        (["penindakan", "operasi", "sitaan", "gagalkan"], "#Penindakan"),
        (["penyelundupan", "smuggling", "ilegal"], "#Penyelundupan"),
        (["rokok ilegal", "rokok tanpa pita cukai"], "#RokokIlegal"),
        (["narkoba", "drug", "meth", "sabu", "kokain"], "#Narkotika"),
        (["miras", "minuman keras"], "#Miras"),
        (["barang kena cukai"], "#BKC"),

        (["tarif", "bea masuk"], "#BeaMasuk"),
        (["pajak", "ppn", "pnbp"], "#PenerimaanNegara"),
        (["cukai"], "#Cukai"),
        (["anti dumping", "bea masuk anti dumping"], "#AntiDumping"),
        (["safeguard"], "#Safeguard"),

        (["aturan", "pmk", "peraturan", "regulasi"], "#Regulasi"),
        (["revisi aturan", "perubahan pmk"], "#PerubahanAturan"),
        (["wco"], "#WCO"),
        (["wto"], "#WTO"),
        (["asean"], "#ASEAN"),
        (["fta", "perjanjian perdagangan"], "#FTA"),
        (["ska", "certificate of origin", "coo"], "#SKA"),

        (["pelabuhan", "tanjung priok"], "#TanjungPriok"),
        (["soekarno hatta", "bandara"], "#Bandara"),
        (["logistik", "supply chain"], "#Logistik"),
        (["container", "peti kemas"], "#Container"),

        (["tembakau"], "#Tembakau"),
        (["rokok"], "#Rokok"),
        (["tekstil", "tpt"], "#Tekstil"),
        (["baja", "steel"], "#Baja"),
        (["otomotif"], "#Otomotif"),
        (["elektronik"], "#Elektronik"),
        (["minyak sawit", "cpo"], "#Sawit"),

        (["transformasi", "digitalisasi"], "#Digitalisasi"),
        (["zona integritas"], "#ZonaIntegritas"),
        (["reformasi birokrasi"], "#ReformasiBirokrasi"),
        (["pengawasan"], "#Pengawasan"),
    ]

    out = []
    for keys, tag in TAGS:
        if any(k in t or k in u for k in keys):
            out.append(tag)

    if not out:
        out = ["#BCNews"]

    return out[:5]


def short_display_url(u: str, max_len: int = 60) -> str:
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
# TELEGRAM (HTML + inline buttons)
# =========================
def telegram_send(session: requests.Session, text: str, reply_markup: dict = None):
    """Send message via Telegram Bot API with optional inline keyboard."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram skipped: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID empty")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)

    r = request_with_retry(
        session,
        "POST",
        url,
        timeout=25,
        json=payload,
    )
    print("Telegram:", r.status_code, (r.text or "")[:140])


def chunk_text(text: str, limit: int = 3500):
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


def send_updates_batched(session: requests.Session, updates):
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


def _send_one_batch(session: requests.Session, batch):
    """Send a batch of articles with richer formatting + inline buttons."""
    for it in batch:
        pub = it.get("published_utc")
        title = (it.get("title") or "").strip()
        url = (it.get("url") or "").strip()
        src = (it.get("source") or "-").strip()
        description = (it.get("description") or "").strip()

        # Sentiment
        sentiment = it.get("sentiment", {})
        sent_emoji = sentiment.get("emoji", "⚪")
        sent_label = sentiment.get("label", "Netral")

        tags = " ".join(make_hashtags(title, url))

        # HTML-escape content
        title_h = html.escape(title)
        src_h = html.escape(src)
        tags_h = html.escape(tags)

        # Build message lines
        lines = [
            f"🛃 <b>BC News Update</b>",
            "",
            f"📰 <b>{title_h}</b>",
        ]

        # Add snippet if available
        if INCLUDE_SNIPPET and description:
            snippet = clean_html_description(description)
            if snippet:
                lines.append(f"📝 <i>{html.escape(snippet)}</i>")

        lines.append("")
        lines.append(f"🕒 {fmt_wib(pub)}")
        lines.append(f"📌 {src_h}")
        lines.append(f"{sent_emoji} Sentimen: <b>{sent_label}</b>")
        lines.append(f"🏷️ {tags_h}")

        text = "\n".join(lines)

        # Inline keyboard buttons
        inline_buttons = []

        if url:
            inline_buttons.append({"text": "📖 Baca Artikel", "url": url})

        # Google search for more context
        if title:
            search_q = quote(title[:80])
            search_url = f"https://www.google.com/search?q={search_q}"
            inline_buttons.append({"text": "🔍 Cari Lebih", "url": search_url})

        reply_markup = None
        if inline_buttons:
            reply_markup = {
                "inline_keyboard": [inline_buttons]
            }

        # Send with inline buttons (each article is its own message)
        for part in chunk_text(text):
            # Only attach buttons to the last chunk
            telegram_send(session, part, reply_markup=reply_markup)


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
        description = (entry.get("summary") or entry.get("description") or "").strip()
        out.append({
            "source": "GoogleNews",
            "title": title,
            "url": resolve_final_url(session, link),
            "published_utc": pub,
            "description": description,
        })
    return out


# =========================
# NEWSAPI (debug-friendly)
# =========================
def fetch_newsapi(session: requests.Session, query: str, cutoff_utc: datetime):
    if not NEWSAPI_KEY:
        if DEBUG_NEWSAPI:
            print("NewsAPI skipped: NEWSAPI_KEY empty")
        return []

    url = "https://newsapi.org/v2/everything"
    cutoff_str = cutoff_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    to_str = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    params = {
        "q": query,
        "searchIn": "title,description",
        "sortBy": "publishedAt",
        "pageSize": NEWSAPI_PAGE_SIZE,
        "apiKey": NEWSAPI_KEY,
        "excludeDomains": NEWSAPI_EXCLUDE_DOMAINS,
        "from": cutoff_str,
        "to": to_str,
    }
    if NEWSAPI_LANGUAGE:
        params["language"] = NEWSAPI_LANGUAGE

    r = request_with_retry(session, "GET", url, params=params, timeout=25)

    if DEBUG_NEWSAPI:
        print("NewsAPI HTTP:", r.status_code)
        print("NewsAPI URL:", r.url)

    try:
        data = r.json()
    except Exception:
        if DEBUG_NEWSAPI:
            print("⚠️ NewsAPI non-JSON body:", (r.text or "")[:400])
        return []

    if data.get("status") != "ok":
        if DEBUG_NEWSAPI:
            print("⚠️ NewsAPI error payload:", data)
        return []

    if DEBUG_NEWSAPI:
        print("NewsAPI totalResults:", data.get("totalResults"))

    out = []
    for a in data.get("articles", []):
        pub = parse_newsapi_datetime(a.get("publishedAt"))
        out.append({
            "source": f"NewsAPI:{(a.get('source', {}) or {}).get('name', '')}".strip(),
            "title": (a.get("title") or "").strip(),
            "url": norm_url(a.get("url") or ""),
            "published_utc": pub,
            "description": (a.get("description") or "").strip(),
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
        by_fp = {}
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

        # Sentiment counters for heartbeat
        sent_counts = {"Positif": 0, "Negatif": 0, "Netral": 0}

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

            # Run sentiment analysis
            description = it.get("description") or ""
            sentiment = analyze_sentiment(title, description)
            it["sentiment"] = sentiment
            sent_counts[sentiment["label"]] = sent_counts.get(sentiment["label"], 0) + 1

            mark_seen(con, fp, url, title)
            new_items.append(it)

        send_updates_batched(session, new_items)

        if SEND_HEARTBEAT:
            sent_summary = " | ".join(
                f"{emoji} {label}: {sent_counts.get(label, 0)}"
                for label, emoji in [("Positif", "🟢"), ("Negatif", "🔴"), ("Netral", "⚪")]
            )
            telegram_send(
                session,
                f"✅ BC monitor OK\n"
                f"📊 New: {len(new_items)} | Seen: {seen_skip} | Old: {too_old} | "
                f"No-date: {no_date} | Fetched: {len(items)}\n"
                f"💡 Sentimen: {sent_summary}\n"
                f"⏱️ Window: {MAX_AGE_HOURS}h"
            )

        print(
            f"Done. New={len(new_items)}, seen_skipped={seen_skip}, "
            f"old_skipped={too_old}, no_date_skipped={no_date}, fetched={len(items)}"
        )

    except Exception as e:
        err = f"❌ BC monitor FAILED: {type(e).__name__}: {e}"
        print(err)
        try:
            telegram_send(session, err)
        except Exception:
            pass
        raise
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
