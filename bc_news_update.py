#!/usr/bin/env python3
"""
BC News Monitor v2.0
====================
Usage:
  python bc_monitor.py              # Normal run (fetch + alert)
  python bc_monitor.py digest       # Daily digest summary
  python bc_monitor.py stats        # Weekly/monthly stats dashboard
  python bc_monitor.py export       # Export all articles to CSV

Features:
  - Google News RSS + NewsAPI dual source
  - Sentiment analysis (offline, Indonesian-aware)
  - Fuzzy duplicate detection across sources
  - Priority alerts for critical keywords
  - Source health monitoring
  - Daily digest & stats dashboard
  - CSV export
  - Telegram inline buttons
"""

import os
import re
import csv
import sys
import html
import json
import hashlib
import sqlite3
import requests
import feedparser
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, urlsplit, urlunsplit, parse_qsl, urlencode
from collections import Counter

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

WIB = timezone(timedelta(hours=7))

# Duplicate detection: Jaccard similarity threshold (0.0 - 1.0)
# Headlines with similarity >= this are considered duplicates
DUPLICATE_SIMILARITY_THRESHOLD = 0.55

# Source health: alert if a source returns 0 articles this many consecutive times
SOURCE_HEALTH_FAIL_THRESHOLD = 3

# CSV export path
EXPORT_CSV_PATH = "bc_articles_export.csv"

# =========================
# PRIORITY ALERT KEYWORDS
# =========================
# Articles matching these get urgent formatting
PRIORITY_KEYWORDS = [
    # Narkoba
    "narkoba", "narkotika", "sabu", "kokain", "ganja", "heroin",
    "psikotropika", "meth", "ekstasi",
    # Penyelundupan besar
    "penyelundupan", "selundupkan", "smuggling",
    # Senjata
    "senjata api", "senpi", "amunisi", "bom", "eksplosif",
    # Korupsi internal
    "korupsi", "gratifikasi", "suap", "oknum bea cukai", "pungli",
    # Kebijakan besar
    "moratorium", "larangan impor", "larangan ekspor",
]

# =========================
# ENV VARS (GitHub Secrets)
# =========================
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")  # allow RSS-only
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")         # primary (group)
TELEGRAM_PRIVATE_CHAT_ID = os.environ.get("TELEGRAM_PRIVATE_CHAT_ID", "")  # your personal DM

# All chat IDs that receive news alerts (auto-built from above)
TELEGRAM_ALERT_CHATS = list({c for c in [TELEGRAM_CHAT_ID, TELEGRAM_PRIVATE_CHAT_ID] if c})

# Comma-separated list of allowed chat IDs for COMMANDS (private + groups)
_allowed_raw = os.environ.get("TELEGRAM_ALLOWED_CHATS", "")
TELEGRAM_ALLOWED_CHATS = {c.strip() for c in _allowed_raw.split(",") if c.strip()}
if TELEGRAM_CHAT_ID:
    TELEGRAM_ALLOWED_CHATS.add(TELEGRAM_CHAT_ID)
if TELEGRAM_PRIVATE_CHAT_ID:
    TELEGRAM_ALLOWED_CHATS.add(TELEGRAM_PRIVATE_CHAT_ID)

# =========================
# SENTIMENT ANALYSIS (Free, offline, Indonesian-aware)
# =========================
SENTIMENT_POSITIVE = [
    "meningkat", "melampaui", "melebihi target", "tercapai", "berhasil",
    "capaian positif", "kinerja baik", "pertumbuhan", "naik", "surplus",
    "rekor", "tertinggi", "optimis", "optimistis", "apresiasi",
    "kemudahan", "percepat", "mempercepat", "fasilitasi", "efisien",
    "efisiensi", "inovasi", "reformasi", "modernisasi", "digitalisasi",
    "pelayanan prima", "kemudahan berusaha", "simplifikasi",
    "penghargaan", "raih", "meraih", "prestasi", "kerja sama",
    "kolaborasi", "sinergi", "kemitraan", "dukungan", "mendukung",
    "berhasil gagalkan", "berhasil ungkap", "amankan", "diamankan",
    "berhasil amankan", "selamatkan uang negara", "penerimaan negara",
    "kontribusi", "berkontribusi",
    "stabil", "terjaga", "kondusif", "aman", "terkendali",
]

SENTIMENT_NEGATIVE = [
    "penyelundupan", "selundupkan", "ilegal", "illegal", "pelanggaran",
    "melanggar", "pidana", "tindak pidana", "kriminal", "korupsi",
    "gratifikasi", "suap", "pungutan liar", "pungli",
    "sitaan", "sita", "disita", "tangkap", "ditangkap", "tersangka",
    "terdakwa", "hukuman", "denda", "sanksi", "penjara",
    "keluhan", "keluh", "masalah", "kendala", "hambatan", "tertunda",
    "terlambat", "turun", "menurun", "defisit", "rugi", "kerugian",
    "gagal", "kegagalan", "bocor", "kebocoran", "penyimpangan",
    "narkoba", "narkotika", "sabu", "kokain", "ganja", "heroin",
    "psikotropika", "meth", "ekstasi",
    "ancaman", "bahaya", "risiko tinggi", "krisis", "darurat",
    "meresahkan", "merugikan", "kontrovers", "polemik",
    "dugaan", "diduga",
]

SENTIMENT_NEUTRAL_BOOST = [
    "peraturan", "pmk", "regulasi", "ketentuan", "sosialisasi",
    "rapat", "koordinasi", "kunjungan", "audiensi", "seminar",
    "workshop", "pelatihan", "edukasi",
]


def analyze_sentiment(title: str, summary: str = "") -> dict:
    text = f"{title} {summary}".lower().strip()
    if not text:
        return {"label": "neutral", "score": 0.0, "emoji": "⚪", "keywords": []}

    pos_hits, neg_hits, neu_hits = [], [], []
    for kw in SENTIMENT_POSITIVE:
        if kw.lower() in text:
            pos_hits.append(kw)
    for kw in SENTIMENT_NEGATIVE:
        if kw.lower() in text:
            neg_hits.append(kw)
    for kw in SENTIMENT_NEUTRAL_BOOST:
        if kw.lower() in text:
            neu_hits.append(kw)

    total = len(pos_hits) + len(neg_hits) + len(neu_hits)
    if total == 0:
        return {"label": "neutral", "score": 0.0, "emoji": "⚪", "keywords": []}

    net = (len(pos_hits) - len(neg_hits)) / max(total, 1)
    if net > 0.15:
        label, emoji = "positive", "🟢"
    elif net < -0.15:
        label, emoji = "negative", "🔴"
    else:
        label, emoji = "neutral", "⚪"

    matched = [f"+{k}" for k in pos_hits[:3]] + [f"-{k}" for k in neg_hits[:3]]
    return {"label": label, "score": round(net, 2), "emoji": emoji, "keywords": matched}


# =========================
# DUPLICATE DETECTION (Fuzzy, token-based Jaccard)
# =========================
_STOPWORDS_ID = {
    "dan", "di", "ke", "dari", "yang", "untuk", "dengan", "ini", "itu",
    "pada", "adalah", "akan", "juga", "atau", "tidak", "oleh", "ada",
    "bisa", "sudah", "telah", "lebih", "sangat", "saat", "sedang",
    "secara", "serta", "dalam", "antara", "sebuah", "mereka", "kami",
    "the", "a", "an", "in", "of", "and", "to", "for", "is", "on",
    "with", "at", "by", "as", "its", "be", "has", "was", "are",
}


def _tokenize(text: str) -> set:
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return {t for t in text.split() if len(t) > 2 and t not in _STOPWORDS_ID}


def jaccard_similarity(title_a: str, title_b: str) -> float:
    set_a = _tokenize(title_a)
    set_b = _tokenize(title_b)
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


def find_duplicates(new_item: dict, existing_items: list) -> list:
    title = new_item.get("title", "")
    dupes = []
    for other in existing_items:
        if other.get("url") == new_item.get("url"):
            continue
        sim = jaccard_similarity(title, other.get("title", ""))
        if sim >= DUPLICATE_SIMILARITY_THRESHOLD:
            dupes.append({**other, "_similarity": round(sim, 2)})
    return dupes


# =========================
# PRIORITY DETECTION
# =========================
def check_priority(title: str, summary: str = "") -> dict:
    text = f"{title} {summary}".lower()
    hits = [kw for kw in PRIORITY_KEYWORDS if kw.lower() in text]
    return {"is_priority": True, "matched": hits[:5]} if hits else {"is_priority": False, "matched": []}


# =========================
# HTTP SESSION + RETRY
# =========================
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (bc-news-bot)"})
    return s


def request_with_retry(session, method, url, *, timeout=20, max_tries=3, backoff_s=1.5, **kwargs):
    last_err = None
    for i in range(max_tries):
        try:
            return session.request(method, url, timeout=timeout, **kwargs)
        except Exception as e:
            last_err = e
            if i < max_tries - 1:
                import time
                time.sleep(backoff_s * (2 ** i))
    raise last_err


# =========================
# DATABASE v2
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
    """Init DB with v2 schema: seen + source_health tables."""
    cur = con.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='seen'")
    exists = cur.fetchone() is not None

    if not exists:
        cur.execute("""
            CREATE TABLE seen (
                fingerprint TEXT PRIMARY KEY,
                url TEXT,
                title TEXT,
                first_seen_utc TEXT,
                source TEXT DEFAULT '',
                sentiment_label TEXT DEFAULT '',
                sentiment_score REAL DEFAULT 0.0,
                hashtags TEXT DEFAULT '',
                is_priority INTEGER DEFAULT 0,
                summary TEXT DEFAULT ''
            )
        """)
    else:
        cur.execute("PRAGMA table_info(seen)")
        cols = {row[1] for row in cur.fetchall()}

        if "fingerprint" not in cols:
            print("🔁 Migrating seen.sqlite: old(url) -> new(fingerprint)")
            cur.execute("ALTER TABLE seen RENAME TO seen_old")
            cur.execute("""
                CREATE TABLE seen (
                    fingerprint TEXT PRIMARY KEY,
                    url TEXT, title TEXT, first_seen_utc TEXT,
                    source TEXT DEFAULT '', sentiment_label TEXT DEFAULT '',
                    sentiment_score REAL DEFAULT 0.0, hashtags TEXT DEFAULT '',
                    is_priority INTEGER DEFAULT 0, summary TEXT DEFAULT ''
                )
            """)
            cur.execute("SELECT url, first_seen_utc FROM seen_old")
            for url_val, first_seen in cur.fetchall():
                fp = make_fingerprint(url_val or "", "")
                cur.execute(
                    "INSERT OR IGNORE INTO seen (fingerprint, url, title, first_seen_utc) VALUES (?, ?, ?, ?)",
                    (fp, url_val, "", first_seen),
                )
        else:
            new_cols = {
                "source": "TEXT DEFAULT ''",
                "sentiment_label": "TEXT DEFAULT ''",
                "sentiment_score": "REAL DEFAULT 0.0",
                "hashtags": "TEXT DEFAULT ''",
                "is_priority": "INTEGER DEFAULT 0",
                "summary": "TEXT DEFAULT ''",
            }
            for col_name, col_type in new_cols.items():
                if col_name not in cols:
                    cur.execute(f"ALTER TABLE seen ADD COLUMN {col_name} {col_type}")
                    print(f"  ➕ Added column: seen.{col_name}")

    # source_health table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS source_health (
            source_name TEXT PRIMARY KEY,
            last_success_utc TEXT,
            last_fail_utc TEXT,
            consecutive_fails INTEGER DEFAULT 0,
            total_fetches INTEGER DEFAULT 0,
            total_articles INTEGER DEFAULT 0
        )
    """)

    # bot_state table (tracks Telegram polling offset, etc.)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    con.commit()


def is_seen(con: sqlite3.Connection, fingerprint: str) -> bool:
    return con.cursor().execute("SELECT 1 FROM seen WHERE fingerprint = ?", (fingerprint,)).fetchone() is not None


def mark_seen(con, fp, url, title, source="", sentiment_label="", sentiment_score=0.0,
              hashtags="", is_priority=False, summary=""):
    con.cursor().execute(
        """INSERT OR IGNORE INTO seen
           (fingerprint, url, title, first_seen_utc, source, sentiment_label,
            sentiment_score, hashtags, is_priority, summary)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (fp, url, title, datetime.now(timezone.utc).isoformat(),
         source, sentiment_label, sentiment_score, hashtags, int(is_priority), summary[:500]),
    )
    con.commit()


# =========================
# SOURCE HEALTH MONITORING
# =========================
def record_source_health(con: sqlite3.Connection, source_name: str, article_count: int):
    cur = con.cursor()
    now = datetime.now(timezone.utc).isoformat()

    cur.execute("SELECT consecutive_fails, total_fetches, total_articles FROM source_health WHERE source_name = ?",
                (source_name,))
    row = cur.fetchone()

    if row is None:
        if article_count > 0:
            cur.execute(
                "INSERT INTO source_health (source_name, last_success_utc, consecutive_fails, total_fetches, total_articles) VALUES (?, ?, 0, 1, ?)",
                (source_name, now, article_count))
        else:
            cur.execute(
                "INSERT INTO source_health (source_name, last_fail_utc, consecutive_fails, total_fetches, total_articles) VALUES (?, ?, 1, 1, 0)",
                (source_name, now))
    else:
        consec_fails, total_fetches, total_arts = row
        total_fetches += 1
        total_arts += article_count
        if article_count > 0:
            cur.execute(
                "UPDATE source_health SET last_success_utc=?, consecutive_fails=0, total_fetches=?, total_articles=? WHERE source_name=?",
                (now, total_fetches, total_arts, source_name))
        else:
            cur.execute(
                "UPDATE source_health SET last_fail_utc=?, consecutive_fails=?, total_fetches=?, total_articles=? WHERE source_name=?",
                (now, consec_fails + 1, total_fetches, total_arts, source_name))
    con.commit()


def check_source_health_alerts(con: sqlite3.Connection) -> list:
    cur = con.cursor()
    cur.execute(
        "SELECT source_name, consecutive_fails, last_success_utc FROM source_health WHERE consecutive_fails >= ?",
        (SOURCE_HEALTH_FAIL_THRESHOLD,))
    alerts = []
    for name, fails, last_ok in cur.fetchall():
        alerts.append(
            f"⚠️ <b>{html.escape(name)}</b> returned 0 articles {fails}x in a row. "
            f"Last OK: {html.escape(last_ok or 'never')}")
    return alerts


# =========================
# HELPERS
# =========================
def resolve_final_url(session, u):
    if not u:
        return ""
    u = norm_url(u)
    try:
        r = request_with_retry(session, "HEAD", u, timeout=12, allow_redirects=True)
        if r and getattr(r, "url", None):
            return norm_url(r.url)
    except Exception:
        pass
    try:
        r = request_with_retry(session, "GET", u, timeout=15, allow_redirects=True, stream=True)
        if r and getattr(r, "url", None):
            return norm_url(r.url)
    except Exception:
        pass
    return u


def fmt_wib(dt_utc):
    if not dt_utc:
        return "Unknown"
    return dt_utc.astimezone(WIB).strftime("%Y-%m-%d %H:%M WIB")


def parse_newsapi_datetime(s):
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
    return out[:5] if out else ["#BCNews"]


def short_display_url(u: str, max_len: int = 60) -> str:
    if not u:
        return ""
    parts = urlsplit(u)
    display = f"{parts.netloc}{parts.path}"
    if parts.query:
        display += "?"
    return display[:max_len - 1] + "…" if len(display) > max_len else display


# =========================
# TELEGRAM
# =========================
# When set, telegram_send replies to this chat only (for bot commands).
# When None, telegram_send broadcasts to ALL TELEGRAM_ALERT_CHATS.
_reply_target_chat_id = None


def _telegram_send_one(session, chat_id, text, reply_markup=None):
    """Send a message to a single chat."""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    r = request_with_retry(
        session, "POST",
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        timeout=25, json=payload)
    print(f"Telegram [{chat_id}]:", r.status_code, (r.text or "")[:120])


def telegram_send(session, text, reply_markup=None):
    """Send message. If replying to a command → single chat. Otherwise → all alert chats."""
    if not TELEGRAM_BOT_TOKEN:
        print("⚠️ Telegram skipped: no bot token")
        return

    if _reply_target_chat_id:
        # Responding to a bot command → reply only to that chat
        _telegram_send_one(session, _reply_target_chat_id, text, reply_markup)
    else:
        # Normal alert/heartbeat → broadcast to all configured chats
        targets = TELEGRAM_ALERT_CHATS or ([TELEGRAM_CHAT_ID] if TELEGRAM_CHAT_ID else [])
        if not targets:
            print("⚠️ Telegram skipped: no chat IDs configured")
            return
        for chat_id in targets:
            _telegram_send_one(session, chat_id, text, reply_markup)


def chunk_text(text: str, limit: int = 3500):
    if len(text) <= limit:
        return [text]
    chunks, cur = [], ""
    for line in text.splitlines(True):
        if len(cur) + len(line) > limit:
            chunks.append(cur)
            cur = ""
        cur += line
    if cur:
        chunks.append(cur)
    return chunks


def build_inline_keyboard(buttons):
    return {"inline_keyboard": [[btn] for btn in buttons]}


# =========================
# SEND ARTICLE ALERTS
# =========================
def send_updates_batched(session, updates, all_new_items=None):
    if not updates:
        return
    for it in updates:
        _send_single_article(session, it, all_new_items or updates)


def _send_single_article(session, it, all_items):
    pub = it.get("published_utc")
    title = (it.get("title") or "").strip()
    url = (it.get("url") or "").strip()
    src = (it.get("source") or "-").strip()
    sentiment = it.get("sentiment", {})
    priority = it.get("priority", {})

    tags = " ".join(make_hashtags(title, url))
    title_h = html.escape(title)
    src_h = html.escape(src)
    tags_h = html.escape(tags)

    # Sentiment line
    sent_emoji = sentiment.get("emoji", "⚪")
    sent_label = sentiment.get("label", "neutral").capitalize()
    sent_kws = sentiment.get("keywords", [])
    sent_line = f"{sent_emoji} <b>{html.escape(sent_label)}</b>"
    if sent_kws:
        sent_line += f"  <i>({html.escape(', '.join(sent_kws[:4]))})</i>"

    # Priority header
    if priority.get("is_priority"):
        header = "🚨 <b>PRIORITY BC News Alert</b> 🚨"
        prio_line = f"⚡ <b>Alert:</b> <i>{html.escape(', '.join(priority['matched'][:3]))}</i>"
    else:
        header = "🛃 <b>BC News Update</b>"
        prio_line = None

    # Duplicate cross-reference
    dupes = find_duplicates(it, [x for x in all_items if x is not it])
    dupe_line = None
    if dupes:
        dupe_sources = [d.get("source", "?") for d in dupes[:2]]
        dupe_line = f"🔁 <i>Also covered by: {html.escape(', '.join(dupe_sources))}</i>"

    # Compose
    lines = [header, "", f"📰 <b>{title_h}</b>", ""]
    lines.append(f"🕒 {fmt_wib(pub)}")
    lines.append(f"📌 {src_h}")
    lines.append(f"📊 {sent_line}")
    lines.append(f"🏷️ {tags_h}")
    if prio_line:
        lines.append(prio_line)
    if dupe_line:
        lines.append(dupe_line)

    text = "\n".join(lines)

    buttons = [{"text": "📖 Baca Artikel", "url": url}] if url else []
    reply_markup = build_inline_keyboard(buttons) if buttons else None

    for part in chunk_text(text):
        telegram_send(session, part, reply_markup=reply_markup)


# =========================
# GOOGLE NEWS RSS
# =========================
def fetch_google_news_rss(session, query):
    rss_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=id&gl=ID&ceid=ID:id"
    feed = feedparser.parse(rss_url)
    out = []
    for entry in feed.entries[:GOOGLE_RSS_SIZE]:
        pub = entry_published_utc(entry)
        out.append({
            "source": "GoogleNews",
            "title": (entry.get("title") or "").strip(),
            "summary": (entry.get("summary") or "").strip(),
            "url": resolve_final_url(session, entry.get("link") or ""),
            "published_utc": pub,
        })
    return out


# =========================
# NEWSAPI
# =========================
def fetch_newsapi(session, query, cutoff_utc):
    if not NEWSAPI_KEY:
        if DEBUG_NEWSAPI:
            print("NewsAPI skipped: NEWSAPI_KEY empty")
        return []

    params = {
        "q": query, "searchIn": "title,description", "sortBy": "publishedAt",
        "pageSize": NEWSAPI_PAGE_SIZE, "apiKey": NEWSAPI_KEY,
        "excludeDomains": NEWSAPI_EXCLUDE_DOMAINS,
        "from": cutoff_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "to": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    if NEWSAPI_LANGUAGE:
        params["language"] = NEWSAPI_LANGUAGE

    r = request_with_retry(session, "GET", "https://newsapi.org/v2/everything", params=params, timeout=25)

    if DEBUG_NEWSAPI:
        print("NewsAPI HTTP:", r.status_code, "URL:", r.url)

    try:
        data = r.json()
    except Exception:
        return []

    if data.get("status") != "ok":
        if DEBUG_NEWSAPI:
            print("⚠️ NewsAPI error:", data)
        return []

    out = []
    for a in data.get("articles", []):
        pub = parse_newsapi_datetime(a.get("publishedAt"))
        out.append({
            "source": f"NewsAPI:{(a.get('source', {}) or {}).get('name', '')}".strip(),
            "title": (a.get("title") or "").strip(),
            "summary": (a.get("description") or "").strip(),
            "url": norm_url(a.get("url") or ""),
            "published_utc": pub,
        })
    return out


# =============================================================================
# COMMAND: NORMAL RUN
# =============================================================================
def cmd_run():
    session = build_session()
    con = sqlite3.connect(DB_FILE)

    try:
        init_db(con)
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(hours=MAX_AGE_HOURS)

        # Fetch & track health
        rss_items = fetch_google_news_rss(session, QUERY_RSS)
        record_source_health(con, "GoogleNews RSS", len(rss_items))

        api_items = fetch_newsapi(session, QUERY_NEWSAPI, cutoff)
        record_source_health(con, "NewsAPI", len(api_items))

        items = rss_items + api_items

        # Deduplicate by fingerprint
        by_fp = {}
        for it in items:
            if not it.get("url") and not it.get("title"):
                continue
            fp = make_fingerprint(it.get("url", ""), it.get("title", ""))
            if fp not in by_fp:
                by_fp[fp] = it
            else:
                old = by_fp[fp]
                old_pub, new_pub = old.get("published_utc"), it.get("published_utc")
                if (old_pub is None and new_pub) or (old_pub and new_pub and new_pub > old_pub):
                    by_fp[fp] = it

        items = sorted(
            by_fp.values(),
            key=lambda x: x.get("published_utc") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True)

        new_items = []
        too_old = no_date = seen_skip = 0
        sent_counts = {"positive": 0, "negative": 0, "neutral": 0}

        for it in items:
            pub = it.get("published_utc")
            if pub is None:
                no_date += 1
                continue
            if pub < cutoff:
                too_old += 1
                continue

            url, title = it.get("url", ""), it.get("title", "")
            fp = make_fingerprint(url, title)
            if is_seen(con, fp):
                seen_skip += 1
                continue

            # Enrich
            summary = it.get("summary", "")
            sentiment = analyze_sentiment(title, summary)
            priority = check_priority(title, summary)
            tags_str = " ".join(make_hashtags(title, url))

            it["sentiment"] = sentiment
            it["priority"] = priority
            sent_counts[sentiment["label"]] += 1

            mark_seen(con, fp, url, title,
                      source=it.get("source", ""),
                      sentiment_label=sentiment["label"],
                      sentiment_score=sentiment["score"],
                      hashtags=tags_str,
                      is_priority=priority["is_priority"],
                      summary=summary)
            new_items.append(it)

        # Send: priority first, then normal
        prio = [x for x in new_items if x.get("priority", {}).get("is_priority")]
        normal = [x for x in new_items if not x.get("priority", {}).get("is_priority")]
        send_updates_batched(session, prio, all_new_items=new_items)
        send_updates_batched(session, normal, all_new_items=new_items)

        # Source health alerts
        for alert in check_source_health_alerts(con):
            telegram_send(session, alert)

        # Heartbeat
        if SEND_HEARTBEAT:
            telegram_send(
                session,
                f"✅ BC monitor OK. New: {len(new_items)} (🚨{len(prio)}). "
                f"Sentiment: 🟢{sent_counts['positive']} 🔴{sent_counts['negative']} ⚪{sent_counts['neutral']}. "
                f"Seen: {seen_skip}. Old: {too_old}. No-date: {no_date}. "
                f"Fetched: {len(items)}. Window: {MAX_AGE_HOURS}h.")

        print(f"Done. New={len(new_items)} (prio={len(prio)}), seen={seen_skip}, old={too_old}, "
              f"no_date={no_date}, fetched={len(items)}, sentiment={sent_counts}")

    except Exception as e:
        err = f"❌ BC monitor FAILED: {type(e).__name__}: {e}"
        print(err)
        try:
            telegram_send(session, err)
        except Exception:
            pass
        raise
    finally:
        con.close()


# =============================================================================
# COMMAND: DAILY DIGEST
# =============================================================================
def cmd_digest():
    session = build_session()
    con = sqlite3.connect(DB_FILE)

    try:
        init_db(con)
        cur = con.cursor()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

        cur.execute(
            """SELECT title, url, source, sentiment_label, hashtags, is_priority, first_seen_utc
               FROM seen WHERE first_seen_utc >= ? ORDER BY first_seen_utc DESC""",
            (cutoff,))
        rows = cur.fetchall()

        if not rows:
            telegram_send(session, "📋 <b>Daily Digest</b>\n\nTidak ada artikel dalam 24 jam terakhir.")
            return

        by_sent = {"positive": [], "negative": [], "neutral": []}
        prio_items = []

        for title, url, source, sent, tags, is_prio, seen_utc in rows:
            item = {"title": title, "url": url, "source": source, "tags": tags}
            by_sent.setdefault(sent or "neutral", []).append(item)
            if is_prio:
                prio_items.append(item)

        now_wib = datetime.now(timezone.utc).astimezone(WIB).strftime("%d %b %Y")
        total = len(rows)
        pos, neg, neu = len(by_sent.get("positive", [])), len(by_sent.get("negative", [])), len(by_sent.get("neutral", []))

        lines = [
            f"📋 <b>Daily Digest — {now_wib}</b>",
            "",
            f"📊 Total: <b>{total}</b> artikel",
            f"   🟢 Positif: {pos}  |  🔴 Negatif: {neg}  |  ⚪ Netral: {neu}",
            f"   🚨 Prioritas: {len(prio_items)}",
        ]

        if prio_items:
            lines += ["", "🚨 <b>Artikel Prioritas:</b>"]
            for item in prio_items[:10]:
                t = html.escape((item["title"] or "")[:80])
                u = html.escape(item["url"] or "")
                lines.append(f'  • <a href="{u}">{t}</a>')

        for label, emoji_c in [("negative", "🔴"), ("positive", "🟢"), ("neutral", "⚪")]:
            items = by_sent.get(label, [])
            if not items:
                continue
            lines += ["", f"{emoji_c} <b>{label.capitalize()} ({len(items)}):</b>"]
            for item in items[:7]:
                t = html.escape((item["title"] or "")[:80])
                u = html.escape(item["url"] or "")
                src = html.escape((item["source"] or "")[:20])
                lines.append(f'  • <a href="{u}">{t}</a> <i>({src})</i>')
            if len(items) > 7:
                lines.append(f"  ... +{len(items) - 7} lainnya")

        for part in chunk_text("\n".join(lines), 3500):
            telegram_send(session, part)

        print(f"Digest: {total} articles (pos={pos}, neg={neg}, neu={neu}, prio={len(prio_items)})")

    except Exception as e:
        err = f"❌ Digest FAILED: {type(e).__name__}: {e}"
        print(err)
        try:
            telegram_send(session, err)
        except Exception:
            pass
        raise
    finally:
        con.close()


# =============================================================================
# COMMAND: STATS DASHBOARD
# =============================================================================
def cmd_stats():
    session = build_session()
    con = sqlite3.connect(DB_FILE)

    try:
        init_db(con)
        cur = con.cursor()
        now_utc = datetime.now(timezone.utc)
        week_cut = (now_utc - timedelta(days=7)).isoformat()
        month_cut = (now_utc - timedelta(days=30)).isoformat()

        # Weekly
        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        wk_total = cur.fetchone()[0]

        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label",
                    (week_cut,))
        wk_sent = dict(cur.fetchall())

        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ? AND is_priority = 1", (week_cut,))
        wk_prio = cur.fetchone()[0]

        # Top hashtags
        cur.execute("SELECT hashtags FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        tag_counter = Counter()
        for (t,) in cur.fetchall():
            for tag in (t or "").split():
                if tag.startswith("#"):
                    tag_counter[tag] += 1
        top_tags = tag_counter.most_common(10)

        # Top sources
        cur.execute(
            "SELECT source, COUNT(*) c FROM seen WHERE first_seen_utc >= ? GROUP BY source ORDER BY c DESC LIMIT 10",
            (week_cut,))
        top_sources = cur.fetchall()

        # Daily trend (7 days)
        daily = []
        for d in range(6, -1, -1):
            ds = (now_utc - timedelta(days=d)).replace(hour=0, minute=0, second=0).isoformat()
            de = (now_utc - timedelta(days=d)).replace(hour=23, minute=59, second=59).isoformat()
            cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc <= ?", (ds, de))
            daily.append(((now_utc - timedelta(days=d)).strftime("%a"), cur.fetchone()[0]))

        # Monthly
        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ?", (month_cut,))
        mo_total = cur.fetchone()[0]
        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label",
                    (month_cut,))
        mo_sent = dict(cur.fetchall())

        # Source health
        cur.execute("SELECT source_name, consecutive_fails, total_fetches, total_articles FROM source_health")
        health_rows = cur.fetchall()

        # Build message
        now_wib = now_utc.astimezone(WIB).strftime("%d %b %Y %H:%M WIB")
        lines = [
            f"📊 <b>Stats Dashboard — {now_wib}</b>",
            "",
            "━━━ <b>Minggu Ini (7 hari)</b> ━━━",
            f"📰 Total: <b>{wk_total}</b>",
            f"🟢 {wk_sent.get('positive', 0)}  |  🔴 {wk_sent.get('negative', 0)}  |  ⚪ {wk_sent.get('neutral', 0)}",
            f"🚨 Prioritas: {wk_prio}",
        ]

        if daily:
            mx = max(c for _, c in daily) or 1
            lines += ["", "<b>Tren harian:</b>"]
            for lbl, cnt in daily:
                bar = "█" * int(cnt / mx * 10) + "░" * (10 - int(cnt / mx * 10))
                lines.append(f"  {lbl} {bar} {cnt}")

        if top_tags:
            lines += ["", "<b>Topik terbanyak:</b>"]
            for tag, cnt in top_tags[:8]:
                lines.append(f"  {html.escape(tag)}: {cnt}")

        if top_sources:
            lines += ["", "<b>Sumber terbanyak:</b>"]
            for src, cnt in top_sources[:5]:
                lines.append(f"  {html.escape(src)}: {cnt}")

        lines += [
            "", "━━━ <b>Bulan Ini (30 hari)</b> ━━━",
            f"📰 Total: <b>{mo_total}</b>",
            f"🟢 {mo_sent.get('positive', 0)}  |  🔴 {mo_sent.get('negative', 0)}  |  ⚪ {mo_sent.get('neutral', 0)}",
        ]

        if health_rows:
            lines += ["", "━━━ <b>Source Health</b> ━━━"]
            for name, fails, tf, ta in health_rows:
                status = "✅" if fails == 0 else f"⚠️ ({fails}x gagal)"
                avg = round(ta / tf, 1) if tf > 0 else 0
                lines.append(f"  {html.escape(name)}: {status} — avg {avg}/fetch")

        for part in chunk_text("\n".join(lines), 3500):
            telegram_send(session, part)

        print(f"Stats: week={wk_total}, month={mo_total}")

    except Exception as e:
        err = f"❌ Stats FAILED: {type(e).__name__}: {e}"
        print(err)
        try:
            telegram_send(session, err)
        except Exception:
            pass
        raise
    finally:
        con.close()


# =============================================================================
# COMMAND: EXPORT CSV
# =============================================================================
def cmd_export(chat_id_override: str = None):
    """Export articles to CSV. If chat_id_override, send file to that chat."""
    con = sqlite3.connect(DB_FILE)
    try:
        init_db(con)
        cur = con.cursor()
        cur.execute(
            """SELECT fingerprint, url, title, first_seen_utc, source,
                      sentiment_label, sentiment_score, hashtags, is_priority, summary
               FROM seen ORDER BY first_seen_utc DESC""")
        rows = cur.fetchall()

        with open(EXPORT_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["fingerprint", "url", "title", "first_seen_utc", "source",
                        "sentiment_label", "sentiment_score", "hashtags", "is_priority", "summary"])
            w.writerows(rows)

        print(f"✅ Exported {len(rows)} articles to {EXPORT_CSV_PATH}")

        session = build_session()
        target = chat_id_override or TELEGRAM_CHAT_ID
        if target and os.path.exists(EXPORT_CSV_PATH):
            telegram_send_document(session, EXPORT_CSV_PATH,
                                   caption=f"📁 CSV export: {len(rows)} artikel",
                                   chat_id=target)
        else:
            telegram_send(session, f"📁 CSV export: {len(rows)} artikel → <code>{EXPORT_CSV_PATH}</code>")

    except Exception as e:
        print(f"❌ Export FAILED: {type(e).__name__}: {e}")
        raise
    finally:
        con.close()


# =============================================================================
# TELEGRAM BOT COMMANDS (via polling)
# =============================================================================
TELEGRAM_COMMANDS = {
    "/start": "Tampilkan bantuan",
    "/help": "Tampilkan bantuan",
    "/stats": "Statistik mingguan & bulanan",
    "/digest": "Rangkuman berita 24 jam terakhir",
    "/export": "Export semua artikel ke CSV",
    "/health": "Cek status sumber berita",
    "/sentiment": "Ringkasan sentimen hari ini",
}


def telegram_get_updates(session: requests.Session, offset: int = None) -> list:
    """Fetch new messages via Telegram getUpdates (long polling disabled, instant)."""
    if not TELEGRAM_BOT_TOKEN:
        return []
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"timeout": 0, "allowed_updates": '["message"]'}
    if offset:
        params["offset"] = offset
    try:
        r = request_with_retry(session, "GET", url, params=params, timeout=15)
        data = r.json()
        return data.get("result", [])
    except Exception as e:
        print(f"⚠️ getUpdates failed: {e}")
        return []


def telegram_send_document(session: requests.Session, filepath: str, caption: str = "",
                           chat_id: str = None):
    """Send a file as a Telegram document."""
    target = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target:
        print("⚠️ Telegram doc skipped: creds empty")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        with open(filepath, "rb") as f:
            r = request_with_retry(
                session, "POST", url, timeout=30,
                data={"chat_id": target, "caption": caption, "parse_mode": "HTML"},
                files={"document": (os.path.basename(filepath), f)},
            )
        print("Telegram doc:", r.status_code, (r.text or "")[:140])
    except Exception as e:
        print(f"⚠️ Telegram doc failed: {e}")


def get_bot_state(con: sqlite3.Connection, key: str, default: str = "") -> str:
    cur = con.cursor()
    cur.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
    row = cur.fetchone()
    return row[0] if row else default


def set_bot_state(con: sqlite3.Connection, key: str, value: str):
    con.cursor().execute(
        "INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)", (key, value))
    con.commit()


def handle_bot_command(session: requests.Session, command: str, chat_id: str, con: sqlite3.Connection):
    """Process a single bot command and reply to the originating chat."""
    global _reply_target_chat_id
    _reply_target_chat_id = chat_id  # route all telegram_send to this chat

    try:
        cmd = command.strip().lower().split("@")[0]  # strip @botname suffix

        if cmd in ("/start", "/help"):
            lines = ["🛃 <b>BC News Bot — Commands</b>\n"]
            for c, desc in TELEGRAM_COMMANDS.items():
                lines.append(f"  {c} — {desc}")
            lines.append("\n💡 Bot memeriksa perintah setiap 5 menit.")
            telegram_send(session, "\n".join(lines))

        elif cmd == "/stats":
            cmd_stats()

        elif cmd == "/digest":
            cmd_digest()

        elif cmd == "/export":
            cmd_export(chat_id_override=chat_id)

        elif cmd == "/health":
            _handle_health_command(session, con)

        elif cmd == "/sentiment":
            _handle_sentiment_command(session, con)

        else:
            telegram_send(session, f"❓ Perintah tidak dikenal: <code>{html.escape(cmd)}</code>\nKetik /help untuk daftar perintah.")
    finally:
        _reply_target_chat_id = None  # reset after command


def _handle_health_command(session: requests.Session, con: sqlite3.Connection):
    """Reply with current source health status."""
    cur = con.cursor()
    cur.execute("SELECT source_name, consecutive_fails, total_fetches, total_articles, last_success_utc FROM source_health")
    rows = cur.fetchall()

    if not rows:
        telegram_send(session, "📡 <b>Source Health</b>\n\nBelum ada data. Jalankan fetch terlebih dahulu.")
        return

    lines = ["📡 <b>Source Health</b>\n"]
    for name, fails, tf, ta, last_ok in rows:
        status = "✅ OK" if fails == 0 else f"⚠️ GAGAL ({fails}x berturut-turut)"
        avg = round(ta / tf, 1) if tf > 0 else 0
        last_ok_str = last_ok[:19] if last_ok else "never"
        lines.append(f"<b>{html.escape(name)}</b>")
        lines.append(f"  Status: {status}")
        lines.append(f"  Avg artikel/fetch: {avg}")
        lines.append(f"  Total fetch: {tf}")
        lines.append(f"  Last OK: {html.escape(last_ok_str)}")
        lines.append("")
    telegram_send(session, "\n".join(lines))


def _handle_sentiment_command(session: requests.Session, con: sqlite3.Connection):
    """Reply with today's sentiment summary."""
    cur = con.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label",
                (cutoff,))
    counts = dict(cur.fetchall())

    total = sum(counts.values())
    pos = counts.get("positive", 0)
    neg = counts.get("negative", 0)
    neu = counts.get("neutral", 0)

    # Top negative articles
    cur.execute(
        "SELECT title, url FROM seen WHERE first_seen_utc >= ? AND sentiment_label = 'negative' ORDER BY first_seen_utc DESC LIMIT 5",
        (cutoff,))
    neg_articles = cur.fetchall()

    # Top positive articles
    cur.execute(
        "SELECT title, url FROM seen WHERE first_seen_utc >= ? AND sentiment_label = 'positive' ORDER BY first_seen_utc DESC LIMIT 5",
        (cutoff,))
    pos_articles = cur.fetchall()

    lines = [
        f"📊 <b>Sentimen 24 Jam Terakhir</b>\n",
        f"Total: <b>{total}</b> artikel",
        f"🟢 Positif: {pos}  |  🔴 Negatif: {neg}  |  ⚪ Netral: {neu}",
    ]

    if neg_articles:
        lines += ["", "🔴 <b>Negatif terbaru:</b>"]
        for title, url in neg_articles:
            t = html.escape((title or "")[:70])
            u = html.escape(url or "")
            lines.append(f'  • <a href="{u}">{t}</a>')

    if pos_articles:
        lines += ["", "🟢 <b>Positif terbaru:</b>"]
        for title, url in pos_articles:
            t = html.escape((title or "")[:70])
            u = html.escape(url or "")
            lines.append(f'  • <a href="{u}">{t}</a>')

    telegram_send(session, "\n".join(lines))


def cmd_poll():
    """Check for new Telegram commands and process them."""
    session = build_session()
    con = sqlite3.connect(DB_FILE)

    try:
        init_db(con)
        last_offset = int(get_bot_state(con, "tg_update_offset", "0"))

        updates = telegram_get_updates(session, offset=last_offset or None)
        if not updates:
            print("Poll: no new messages.")
            return

        processed = 0
        for update in updates:
            update_id = update.get("update_id", 0)
            msg = update.get("message", {})
            text = (msg.get("text") or "").strip()
            chat_id = str(msg.get("chat", {}).get("id", ""))

            # Security: only respond to allowed chats (private + groups)
            if chat_id not in TELEGRAM_ALLOWED_CHATS:
                print(f"Poll: ignoring message from chat {chat_id}")
                set_bot_state(con, "tg_update_offset", str(update_id + 1))
                continue

            if text.startswith("/"):
                print(f"Poll: processing command '{text}' from chat {chat_id}")
                try:
                    handle_bot_command(session, text, chat_id, con)
                except Exception as e:
                    telegram_send(session, f"❌ Error: <code>{html.escape(str(e)[:200])}</code>")
                processed += 1

            # Always advance offset
            set_bot_state(con, "tg_update_offset", str(update_id + 1))

        print(f"Poll: {len(updates)} updates, {processed} commands processed.")

    except Exception as e:
        print(f"❌ Poll FAILED: {type(e).__name__}: {e}")
    finally:
        con.close()


# =============================================================================
# COMMAND: SETUP (register Telegram bot menu)
# =============================================================================
def cmd_setup():
    """Register bot commands with Telegram so the menu appears in chats."""
    session = build_session()
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN not set.")
        return

    # Commands to register (excluding /start and /help, Telegram handles those)
    commands = [
        {"command": "help", "description": "Tampilkan daftar perintah"},
        {"command": "stats", "description": "Statistik mingguan & bulanan"},
        {"command": "digest", "description": "Rangkuman berita 24 jam terakhir"},
        {"command": "export", "description": "Export semua artikel ke CSV"},
        {"command": "health", "description": "Cek status sumber berita"},
        {"command": "sentiment", "description": "Ringkasan sentimen hari ini"},
    ]

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMyCommands"

    # Register for all chats (default scope)
    try:
        r = request_with_retry(session, "POST", url, timeout=15,
                               json={"commands": commands})
        data = r.json()
        if data.get("ok"):
            print("✅ Bot commands registered (all chats).")
        else:
            print(f"⚠️ setMyCommands failed: {data}")
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return

    # Also register specifically for groups
    try:
        r = request_with_retry(session, "POST", url, timeout=15,
                               json={
                                   "commands": commands,
                                   "scope": {"type": "all_group_chats"},
                               })
        data = r.json()
        if data.get("ok"):
            print("✅ Bot commands registered (groups).")
    except Exception as e:
        print(f"⚠️ Group menu registration failed: {e}")

    print("\n📋 Menu registered! Your bot now shows these commands:")
    for c in commands:
        print(f"  /{c['command']} — {c['description']}")
    print("\n💡 Users can tap the '/' button or menu icon in Telegram to see them.")


# =============================================================================
# MAIN
# =============================================================================
def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    cmds = {
        "run": cmd_run,
        "digest": cmd_digest,
        "stats": cmd_stats,
        "export": cmd_export,
        "poll": cmd_poll,
        "setup": cmd_setup,
    }

    if cmd in cmds:
        print(f"▶ {cmd}")
        cmds[cmd]()
    else:
        print(f"Unknown: {cmd}. Available: {', '.join(cmds)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
