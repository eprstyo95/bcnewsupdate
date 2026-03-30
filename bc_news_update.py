#!/usr/bin/env python3
"""
BC News Monitor v3.1
====================
Usage:
  python bc_news_update.py              # Normal run (fetch + alert)
  python bc_news_update.py digest       # Daily digest summary
  python bc_news_update.py stats        # Weekly/monthly stats dashboard
  python bc_news_update.py export       # Export all articles to CSV
  python bc_news_update.py report       # Generate & send weekly PDF report
  python bc_news_update.py leaderboard  # Weekly source & topic leaderboard
  python bc_news_update.py poll         # Check Telegram commands
  python bc_news_update.py setup        # Register Telegram bot menu

Features:
  - Google News RSS (Indonesian + English)
  - Multi-language: Indonesian + English international coverage
  - Sentiment analysis (offline, Indonesian + English)
  - Trending detection (topic spike alerts)
  - Weekly leaderboard (sources & topics)
  - PDF weekly report (auto-generated)
  - Telegram inline buttons + bot commands
  - Two-layer relevance filter (query + post-fetch)  [v3.1]
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

# ── CHANGE 1: Tighter Google News queries ────────────────────────────────────
# Removed standalone "Kemenkeu" / "Kementerian Keuangan" which caught
# APBN, OJK, bond, and tax articles with no customs context.
QUERY_RSS_ID = (
    '"bea cukai" OR "DJBC" OR "kepabeanan" OR "bea masuk" '
    'OR "penyelundupan" OR "cukai rokok" OR "kawasan berikat" '
    'OR "KPPBC" OR "Kanwil DJBC" when:24h'
)

# English queries (international coverage)
QUERY_RSS_EN = (
    '("Indonesia customs" OR "Indonesia tariff" OR "DGCE Indonesia" '
    'OR "Indonesia trade policy" OR "Indonesia import export") when:24h'
)
# ─────────────────────────────────────────────────────────────────────────────

MAX_AGE_HOURS = 24

GOOGLE_RSS_SIZE = 30

# Direct RSS feeds (faster than Google News aggregation)
DIRECT_RSS_FEEDS = {
    "Antara-BC": "https://www.antaranews.com/rss/topik/bea-cukai.xml",
    "Antara-Ekonomi": "https://www.antaranews.com/rss/ekonomi.xml",
    "Detik-Finance": "https://rss.detik.com/index.php/finance",
    "Kompas-Ekonomi": "https://rss.kompas.com/ekonomi",
    "Bisnis-Ekonomi": "https://www.bisnis.com/rss/ekonomi",
    "CNBC-ID": "https://www.cnbcindonesia.com/rss",
    "Kontan": "https://www.kontan.co.id/rss",
    "Tempo-Bisnis": "https://rss.tempo.co/bisnis",
}

# ── CHANGE 3: Tighter direct RSS keywords ────────────────────────────────────
# Removed standalone "impor", "ekspor", "tarif" which matched thousands of
# unrelated trade/economics articles. Now requires customs-specific context.
DIRECT_RSS_KEYWORDS = [
    "bea cukai", "djbc", "kepabeanan",
    "cukai", "bea masuk", "bea keluar",
    "penyelundupan", "smuggling", "customs",
    "pita cukai", "rokok ilegal", "miras ilegal",
    "kawasan berikat", "plb", "kite", "gudang berikat",
    "kppbc", "kanwil djbc", "kantor bea",
    "narkoba bea",              # enforcement context
    "tarif bea",                # not just "tarif" alone
    "impor ilegal", "ekspor ilegal",  # not just "impor" alone
    "barang selundupan", "barang ilegal",
    "pengawasan kepabeanan",
]
# ─────────────────────────────────────────────────────────────────────────────

# ── CHANGE 2: Post-fetch relevance filter (applied to ALL sources) ────────────
# Google News results had zero filtering — these two lists fix that.

RELEVANCE_REQUIRED_KEYWORDS = [
    # Core BC terms
    "bea cukai", "djbc", "kepabeanan", "cukai",
    "bea masuk", "bea keluar", "penyelundupan", "smuggling",
    "kawasan berikat", "plb", "kite", "gudang berikat",
    "kppbc", "kanwil djbc", "kantor bea",
    # Enforcement
    "pita cukai", "rokok ilegal", "miras ilegal",
    "narkoba bea", "penindakan bea", "sitaan bea",
    # Trade-policy with customs angle
    "tarif bea", "bea anti dumping", "safeguard bea",
    "impor ilegal", "ekspor ilegal", "barang selundupan",
    # English
    "indonesia customs", "customs indonesia",
    "dgce", "directorate general of customs",
    "customs excise indonesia",
]

RELEVANCE_BLOCKLIST = [
    # Tax / fiscal topics with no customs link
    "pajak penghasilan", "pph 21", "pph badan", "pph pasal",
    "ppn masukan", "ppn keluaran",
    # Financial regulators
    "ojk", "otoritas jasa keuangan",
    "bank indonesia", "bi rate", "suku bunga acuan",
    # Government securities / budget (no customs angle)
    "sbsn", "sbn", "obligasi negara", "surat utang negara",
    "defisit anggaran", "utang negara", "apbn murni",
    # Capital markets
    "saham", "ihsg", "bursa efek indonesia", "pasar modal",
    # SOEs unrelated to trade
    "pln listrik", "pertamina bbm",
]


def is_relevant(title: str, description: str = "") -> bool:
    """
    Two-layer relevance check for ALL fetched items.

    Layer 1 — must match at least one RELEVANCE_REQUIRED_KEYWORDS.
    Layer 2 — reject if blocklist terms dominate over relevant hits
              (catches articles that mention bea cukai once in a
               budget table but are really about APBN/OJK/etc.).
    """
    text = f"{title} {description}".lower()

    relevant_hits = sum(1 for kw in RELEVANCE_REQUIRED_KEYWORDS if kw in text)
    if relevant_hits == 0:
        return False

    blocklist_hits = sum(1 for kw in RELEVANCE_BLOCKLIST if kw in text)
    # Reject if blocklist terms equal or outnumber relevant hits
    # AND there's only a weak relevant signal (1 hit)
    if blocklist_hits > 0 and relevant_hits < 2 and blocklist_hits >= relevant_hits:
        return False

    return True
# ─────────────────────────────────────────────────────────────────────────────

MAX_ITEMS_PER_BATCH = 1
SEND_HEARTBEAT = True

INCLUDE_SNIPPET = True
SNIPPET_MAX_CHARS = 150

WIB = timezone(timedelta(hours=7))

# Trending: alert if a hashtag appears >= this many times in TRENDING_WINDOW_HOURS
TRENDING_THRESHOLD = 4
TRENDING_WINDOW_HOURS = 3

# Fuzzy dedup: Jaccard similarity threshold (0.0 - 1.0)
DEDUP_SIMILARITY_THRESHOLD = 0.40

# Export
EXPORT_CSV_PATH = "bc_articles_export.csv"

# PDF report
REPORT_PDF_PATH = "bc_weekly_report.pdf"

# =========================
# ENV VARS (GitHub Secrets)
# =========================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_PRIVATE_CHAT_ID = os.environ.get("TELEGRAM_PRIVATE_CHAT_ID", "")

TELEGRAM_ALERT_CHATS = list({c for c in [TELEGRAM_CHAT_ID, TELEGRAM_PRIVATE_CHAT_ID] if c})

_allowed_raw = os.environ.get("TELEGRAM_ALLOWED_CHATS", "")
TELEGRAM_ALLOWED_CHATS = {c.strip() for c in _allowed_raw.split(",") if c.strip()}
if TELEGRAM_CHAT_ID:
    TELEGRAM_ALLOWED_CHATS.add(TELEGRAM_CHAT_ID)
if TELEGRAM_PRIVATE_CHAT_ID:
    TELEGRAM_ALLOWED_CHATS.add(TELEGRAM_PRIVATE_CHAT_ID)

# =========================
# SENTIMENT ANALYSIS
# =========================
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
    # Enforcement = positive
    "penindakan", "tindakan tegas", "tindak tegas",
    "sitaan", "sita", "disita", "menyita",
    "gagalkan", "berhasil gagalkan", "berhasil ungkap",
    "amankan", "diamankan", "berhasil amankan",
    "tangkap", "tertangkap", "ditangkap", "ditahan",
    "penyelundupan", "selundupkan", "smuggling", "barang selundupan",
    "narkoba", "narkotika", "sabu", "kokain", "ganja",
    "rokok ilegal", "miras ilegal",
    "selamatkan uang negara", "lindungi masyarakat",
    # English positive
    "success", "achievement", "growth", "innovation", "efficient",
    "record", "surplus", "improvement", "collaboration",
    "seized", "arrested", "intercepted", "crackdown",
    "enforcement", "confiscated", "busted",
]

SENTIMENT_NEGATIVE_KW = [
    # Corruption = negative
    "korupsi", "suap", "gratifikasi", "pungli", "pungutan liar",
    "oknum", "oknum bea cukai", "penyalahgunaan",
    "fraud", "pemalsuan", "palsu",
    "dugaan korupsi", "kasus korupsi",
    # Failures
    "gagal", "kegagalan", "masalah", "keluhan", "hambatan",
    "penurunan", "defisit", "rugi", "kerugian negara",
    "bocor", "kebocoran", "penyimpangan",
    "terlambat", "tertunda", "lambat",
    "pelanggaran kode etik", "pelanggaran disiplin",
    "diduga", "dugaan",
    "ancaman", "bahaya", "krisis", "darurat",
    "meresahkan", "merugikan", "kontrovers", "polemik",
    # English negative
    "corruption", "bribery", "fraud", "decline", "deficit",
    "failure", "complaint", "scandal", "mismanagement",
]

SENTIMENT_NEUTRAL_KW = [
    "sosialisasi", "edukasi", "kunjungan", "rapat", "koordinasi",
    "peraturan baru", "pmk", "ketentuan", "audiensi", "mou",
    "workshop", "seminar", "bimtek", "pelatihan",
]


def analyze_sentiment(title: str, description: str = "") -> dict:
    text = f"{title} {description}".lower().strip()
    if not text:
        return {"label": "Netral", "emoji": "⚪", "score": 0.0}

    pos_hits = sum(1 for kw in SENTIMENT_POSITIVE_KW if kw in text)
    neg_hits = sum(1 for kw in SENTIMENT_NEGATIVE_KW if kw in text)
    neu_hits = sum(1 for kw in SENTIMENT_NEUTRAL_KW if kw in text)

    total = pos_hits + neg_hits + neu_hits
    if total == 0:
        return {"label": "Netral", "emoji": "⚪", "score": 0.0}
    if neu_hits > 0 and abs(pos_hits - neg_hits) <= 1:
        return {"label": "Netral", "emoji": "⚪", "score": 0.0}

    score = max(-1.0, min(1.0, (pos_hits - neg_hits) / total))
    if score > 0.15:
        return {"label": "Positif", "emoji": "🟢", "score": round(score, 2)}
    elif score < -0.15:
        return {"label": "Negatif", "emoji": "🔴", "score": round(score, 2)}
    return {"label": "Netral", "emoji": "⚪", "score": round(score, 2)}


# =========================
# FUZZY DUPLICATE DETECTION
# =========================
_STOPWORDS = {
    # Indonesian
    "dan", "di", "ke", "dari", "yang", "untuk", "dengan", "ini", "itu",
    "pada", "adalah", "akan", "juga", "atau", "tidak", "oleh", "ada",
    "bisa", "sudah", "telah", "lebih", "sangat", "saat", "sedang",
    "secara", "serta", "dalam", "antara", "sebuah", "mereka", "kami",
    # English
    "the", "a", "an", "in", "of", "and", "to", "for", "is", "on",
    "with", "at", "by", "as", "its", "be", "has", "was", "are", "this",
}


def _tokenize(text: str) -> set:
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return {t for t in text.split() if len(t) > 2 and t not in _STOPWORDS}


def jaccard_similarity(title_a: str, title_b: str) -> float:
    set_a = _tokenize(title_a)
    set_b = _tokenize(title_b)
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


def find_similar_articles(item: dict, other_items: list) -> list:
    title = item.get("title", "")
    similar = []
    for other in other_items:
        if other is item or other.get("url") == item.get("url"):
            continue
        sim = jaccard_similarity(title, other.get("title", ""))
        if sim >= DEDUP_SIMILARITY_THRESHOLD:
            similar.append({"source": other.get("source", "?"), "similarity": round(sim, 2)})
    return similar


def deduplicate_fuzzy(items: list) -> list:
    unique = []
    seen_titles = []

    for it in items:
        title = it.get("title", "")
        is_dupe = False

        for prev_title, idx in seen_titles:
            sim = jaccard_similarity(title, prev_title)
            if sim >= DEDUP_SIMILARITY_THRESHOLD:
                src = it.get("source", "?")
                unique[idx].setdefault("also_covered_by", []).append(src)
                is_dupe = True
                break

        if not is_dupe:
            unique.append(it)
            seen_titles.append((title, len(unique) - 1))

    return unique


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
# DATABASE v3
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
                summary TEXT DEFAULT '',
                language TEXT DEFAULT 'id'
            )
        """)
    else:
        cur.execute("PRAGMA table_info(seen)")
        cols = {row[1] for row in cur.fetchall()}

        if "fingerprint" not in cols:
            print("🔁 Migrating schema: old(url) -> new(fingerprint)")
            cur.execute("ALTER TABLE seen RENAME TO seen_old")
            cur.execute("""
                CREATE TABLE seen (
                    fingerprint TEXT PRIMARY KEY,
                    url TEXT, title TEXT, first_seen_utc TEXT,
                    source TEXT DEFAULT '', sentiment_label TEXT DEFAULT '',
                    sentiment_score REAL DEFAULT 0.0, hashtags TEXT DEFAULT '',
                    is_priority INTEGER DEFAULT 0, summary TEXT DEFAULT '',
                    language TEXT DEFAULT 'id'
                )
            """)
            cur.execute("SELECT url, first_seen_utc FROM seen_old")
            for url_val, first_seen in cur.fetchall():
                fp = make_fingerprint(url_val or "", "")
                cur.execute(
                    "INSERT OR IGNORE INTO seen (fingerprint, url, title, first_seen_utc) VALUES (?, ?, ?, ?)",
                    (fp, url_val, "", first_seen))
        else:
            new_cols = {
                "source": "TEXT DEFAULT ''",
                "sentiment_label": "TEXT DEFAULT ''",
                "sentiment_score": "REAL DEFAULT 0.0",
                "hashtags": "TEXT DEFAULT ''",
                "is_priority": "INTEGER DEFAULT 0",
                "summary": "TEXT DEFAULT ''",
                "language": "TEXT DEFAULT 'id'",
            }
            for col_name, col_type in new_cols.items():
                if col_name not in cols:
                    cur.execute(f"ALTER TABLE seen ADD COLUMN {col_name} {col_type}")
                    print(f"  ➕ Added column: seen.{col_name}")

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reactions (
            fingerprint TEXT,
            user_id TEXT,
            reaction TEXT,
            reacted_utc TEXT,
            PRIMARY KEY (fingerprint, user_id)
        )
    """)

    cur.execute("DELETE FROM source_health WHERE source_name LIKE '%NewsAPI%'")

    con.commit()


def is_seen(con, fingerprint):
    return con.cursor().execute("SELECT 1 FROM seen WHERE fingerprint = ?", (fingerprint,)).fetchone() is not None


def mark_seen(con, fp, url, title, source="", sentiment_label="", sentiment_score=0.0,
              hashtags="", is_priority=False, summary="", language="id"):
    con.cursor().execute(
        """INSERT OR IGNORE INTO seen
           (fingerprint, url, title, first_seen_utc, source, sentiment_label,
            sentiment_score, hashtags, is_priority, summary, language)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (fp, url, title, datetime.now(timezone.utc).isoformat(),
         source, sentiment_label, sentiment_score, hashtags, int(is_priority),
         summary[:500], language))
    con.commit()


# =========================
# SOURCE HEALTH
# =========================
def record_source_health(con, source_name, article_count):
    cur = con.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("SELECT consecutive_fails, total_fetches, total_articles FROM source_health WHERE source_name = ?",
                (source_name,))
    row = cur.fetchone()
    if row is None:
        if article_count > 0:
            cur.execute("INSERT INTO source_health (source_name, last_success_utc, consecutive_fails, total_fetches, total_articles) VALUES (?, ?, 0, 1, ?)",
                        (source_name, now, article_count))
        else:
            cur.execute("INSERT INTO source_health (source_name, last_fail_utc, consecutive_fails, total_fetches, total_articles) VALUES (?, ?, 1, 1, 0)",
                        (source_name, now))
    else:
        cf, tf, ta = row
        tf += 1
        ta += article_count
        if article_count > 0:
            cur.execute("UPDATE source_health SET last_success_utc=?, consecutive_fails=0, total_fetches=?, total_articles=? WHERE source_name=?",
                        (now, tf, ta, source_name))
        else:
            cur.execute("UPDATE source_health SET last_fail_utc=?, consecutive_fails=?, total_fetches=?, total_articles=? WHERE source_name=?",
                        (now, cf + 1, tf, ta, source_name))
    con.commit()


def check_source_health_alerts(con):
    SILENT_SOURCES = {"GoogleNews-EN"}
    cur = con.cursor()
    cur.execute("SELECT source_name, consecutive_fails, last_success_utc FROM source_health WHERE consecutive_fails >= 3")
    return [
        f"⚠️ <b>{html.escape(name)}</b> returned 0 articles {fails}x in a row. Last OK: {html.escape(last_ok or 'never')}"
        for name, fails, last_ok in cur.fetchall()
        if name not in SILENT_SOURCES
    ]


# =========================
# BOT STATE
# =========================
def get_bot_state(con, key, default=""):
    cur = con.cursor()
    cur.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
    row = cur.fetchone()
    return row[0] if row else default


def set_bot_state(con, key, value):
    con.cursor().execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)", (key, value))
    con.commit()


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


def entry_published_utc(entry):
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    if not t:
        return None
    return datetime(*t[:6], tzinfo=timezone.utc)


def clean_html_description(raw, max_chars=SNIPPET_MAX_CHARS):
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
        (["tarif", "bea masuk", "tariff"], "#BeaMasuk"),
        (["pajak", "ppn", "pnbp"], "#PenerimaanNegara"),
        (["cukai", "excise"], "#Cukai"),
        (["anti dumping", "bea masuk anti dumping", "antidumping"], "#AntiDumping"),
        (["safeguard"], "#Safeguard"),
        (["aturan", "pmk", "peraturan", "regulasi", "regulation"], "#Regulasi"),
        (["revisi aturan", "perubahan pmk"], "#PerubahanAturan"),
        (["wco"], "#WCO"),
        (["wto"], "#WTO"),
        (["asean"], "#ASEAN"),
        (["fta", "perjanjian perdagangan", "free trade", "trade agreement"], "#FTA"),
        (["ska", "certificate of origin", "coo"], "#SKA"),
        (["pelabuhan", "tanjung priok", "port"], "#TanjungPriok"),
        (["soekarno hatta", "bandara", "airport"], "#Bandara"),
        (["logistik", "supply chain", "logistics"], "#Logistik"),
        (["container", "peti kemas"], "#Container"),
        (["tembakau", "tobacco"], "#Tembakau"),
        (["rokok", "cigarette"], "#Rokok"),
        (["tekstil", "tpt", "textile", "garment"], "#Tekstil"),
        (["baja", "steel"], "#Baja"),
        (["otomotif", "automotive"], "#Otomotif"),
        (["elektronik", "electronic"], "#Elektronik"),
        (["minyak sawit", "cpo", "palm oil"], "#Sawit"),
        (["transformasi", "digitalisasi"], "#Digitalisasi"),
        (["zona integritas"], "#ZonaIntegritas"),
        (["reformasi birokrasi"], "#ReformasiBirokrasi"),
        (["pengawasan"], "#Pengawasan"),
        (["trade war", "trade tension"], "#TradeWar"),
        (["sanctions", "embargo"], "#Sanctions"),
    ]
    out = []
    for keys, tag in TAGS:
        if any(k in t or k in u for k in keys):
            out.append(tag)
    return out[:5] if out else ["#BCNews"]


def short_display_url(u, max_len=60):
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
_reply_target_chat_id = None


def _telegram_send_one(session, chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    r = request_with_retry(session, "POST",
                           f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                           timeout=25, json=payload)
    print(f"Telegram [{chat_id}]:", r.status_code, (r.text or "")[:120])


def telegram_send(session, text, reply_markup=None):
    if not TELEGRAM_BOT_TOKEN:
        print("⚠️ Telegram skipped: no bot token")
        return
    if _reply_target_chat_id:
        _telegram_send_one(session, _reply_target_chat_id, text, reply_markup)
    else:
        targets = TELEGRAM_ALERT_CHATS or ([TELEGRAM_CHAT_ID] if TELEGRAM_CHAT_ID else [])
        for cid in targets:
            _telegram_send_one(session, cid, text, reply_markup)


def telegram_send_document(session, filepath, caption="", chat_id=None):
    target = chat_id or _reply_target_chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        with open(filepath, "rb") as f:
            r = request_with_retry(session, "POST", url, timeout=30,
                                   data={"chat_id": target, "caption": caption, "parse_mode": "HTML"},
                                   files={"document": (os.path.basename(filepath), f)})
        print("Telegram doc:", r.status_code, (r.text or "")[:120])
    except Exception as e:
        print(f"⚠️ Telegram doc failed: {e}")


def chunk_text(text, limit=3500):
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
# TRENDING DETECTION
# =========================
def detect_trending(con):
    cur = con.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=TRENDING_WINDOW_HOURS)).isoformat()

    cur.execute("SELECT hashtags, title, url FROM seen WHERE first_seen_utc >= ?", (cutoff,))
    rows = cur.fetchall()

    tag_counter = Counter()
    tag_articles = {}

    for hashtags_str, title, url in rows:
        for tag in (hashtags_str or "").split():
            if tag.startswith("#"):
                tag_counter[tag] += 1
                tag_articles.setdefault(tag, []).append({"title": title, "url": url})

    trending = []
    for tag, count in tag_counter.most_common(10):
        if count >= TRENDING_THRESHOLD:
            trending.append({
                "tag": tag,
                "count": count,
                "articles": tag_articles[tag][:5],
            })
    return trending


def send_trending_alert(session, trending_topics, con=None, force=False):
    if not trending_topics:
        return

    TRENDING_COOLDOWN_HOURS = 2
    if con and not force:
        last_sent = get_bot_state(con, "trending_last_sent")
        if last_sent:
            try:
                last_dt = datetime.fromisoformat(last_sent)
                if datetime.now(timezone.utc) - last_dt < timedelta(hours=TRENDING_COOLDOWN_HOURS):
                    print(f"Trending: cooldown active (last sent {last_sent}), skipping.")
                    return
            except Exception:
                pass

    lines = [f"🔥 <b>TRENDING — {len(trending_topics)} topik sedang ramai!</b>\n"]

    for t in trending_topics[:5]:
        tag = html.escape(t["tag"])
        count = t["count"]
        lines.append(f"📈 <b>{tag}</b> — {count} artikel dalam {TRENDING_WINDOW_HOURS} jam")
        for art in t["articles"][:3]:
            title_h = html.escape((art["title"] or "")[:70])
            url_h = html.escape(art["url"] or "")
            lines.append(f'  • <a href="{url_h}">{title_h}</a>')
        lines.append("")

    text = "\n".join(lines)
    for part in chunk_text(text):
        telegram_send(session, part)

    if con:
        set_bot_state(con, "trending_last_sent", datetime.now(timezone.utc).isoformat())


# =========================
# SENTIMENT SHIFT DETECTION
# =========================
SENTIMENT_SHIFT_THRESHOLD = 0.25


def detect_sentiment_shift(con) -> str | None:
    today_key = f"shift_alert_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
    if get_bot_state(con, today_key):
        return None

    cur = con.cursor()
    now_utc = datetime.now(timezone.utc)
    this_week_start = (now_utc - timedelta(days=7)).isoformat()
    last_week_start = (now_utc - timedelta(days=14)).isoformat()

    cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label",
                (this_week_start,))
    tw = dict(cur.fetchall())
    tw_total = sum(tw.values())

    cur.execute(
        "SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc < ? GROUP BY sentiment_label",
        (last_week_start, this_week_start))
    lw = dict(cur.fetchall())
    lw_total = sum(lw.values())

    if tw_total < 5 or lw_total < 5:
        return None

    tw_pos_ratio = tw.get("Positif", 0) / tw_total
    tw_neg_ratio = tw.get("Negatif", 0) / tw_total
    lw_pos_ratio = lw.get("Positif", 0) / lw_total
    lw_neg_ratio = lw.get("Negatif", 0) / lw_total

    pos_shift = tw_pos_ratio - lw_pos_ratio
    neg_shift = tw_neg_ratio - lw_neg_ratio

    alerts = []

    if abs(neg_shift) >= SENTIMENT_SHIFT_THRESHOLD:
        if neg_shift > 0:
            alerts.append(f"🔴 Berita <b>negatif naik signifikan</b>: {lw_neg_ratio:.0%} → {tw_neg_ratio:.0%} (+{neg_shift:.0%})")
        else:
            alerts.append(f"🟢 Berita <b>negatif turun signifikan</b>: {lw_neg_ratio:.0%} → {tw_neg_ratio:.0%} ({neg_shift:.0%})")

    if abs(pos_shift) >= SENTIMENT_SHIFT_THRESHOLD:
        if pos_shift > 0:
            alerts.append(f"🟢 Berita <b>positif naik signifikan</b>: {lw_pos_ratio:.0%} → {tw_pos_ratio:.0%} (+{pos_shift:.0%})")
        else:
            alerts.append(f"🔴 Berita <b>positif turun signifikan</b>: {lw_pos_ratio:.0%} → {tw_pos_ratio:.0%} ({pos_shift:.0%})")

    if not alerts:
        return None

    set_bot_state(con, today_key, "1")

    lines = [
        "⚠️ <b>Sentiment Shift Alert</b>", "",
        *alerts, "",
        f"<b>Minggu ini:</b> 🟢{tw.get('Positif', 0)} 🔴{tw.get('Negatif', 0)} ⚪{tw.get('Netral', 0)} (total {tw_total})",
        f"<b>Minggu lalu:</b> 🟢{lw.get('Positif', 0)} 🔴{lw.get('Negatif', 0)} ⚪{lw.get('Netral', 0)} (total {lw_total})",
        "", "💡 Ketik /mediatone untuk detail per outlet."
    ]
    return "\n".join(lines)


# =========================
# SEND ARTICLE ALERTS
# =========================
def send_updates_batched(session, updates):
    for it in updates:
        _send_single_article(session, it)


def _send_single_article(session, it):
    pub = it.get("published_utc")
    title = (it.get("title") or "").strip()
    url = (it.get("url") or "").strip()
    src = (it.get("source") or "-").strip()
    description = (it.get("description") or "").strip()
    sentiment = it.get("sentiment", {})
    lang = it.get("language", "id")

    tags = " ".join(make_hashtags(title, url))
    title_h = html.escape(title)
    src_h = html.escape(src)
    tags_h = html.escape(tags)
    sent_emoji = sentiment.get("emoji", "⚪")
    sent_label = sentiment.get("label", "Netral")

    lang_flag = "🇮🇩" if lang == "id" else "🌐"
    header = "🛃 <b>BC News Update</b>"

    lines = [header, "", f"📰 <b>{title_h}</b>"]

    if INCLUDE_SNIPPET and description:
        snippet = clean_html_description(description)
        if snippet:
            lines.append(f"📝 <i>{html.escape(snippet)}</i>")

    lines.append("")
    lines.append(f"🕒 {fmt_wib(pub)}")
    lines.append(f"📌 {src_h} {lang_flag}")
    lines.append(f"{sent_emoji} Sentimen: <b>{sent_label}</b>")
    lines.append(f"🏷️ {tags_h}")

    also_covered = it.get("also_covered_by", [])
    if also_covered:
        sources_str = ", ".join(html.escape(s) for s in also_covered[:3])
        lines.append(f"🔁 <i>Also: {sources_str}</i>")

    text = "\n".join(lines)

    fp = make_fingerprint(url, title)

    keyboard = []
    url_row = []
    if url:
        url_row.append({"text": "📖 Baca Artikel", "url": url})
    if title:
        search_url = f"https://www.google.com/search?q={quote(title[:80])}"
        url_row.append({"text": "🔍 Cari Lebih", "url": search_url})
    if url_row:
        keyboard.append(url_row)

    keyboard.append([
        {"text": "👍 Relevan", "callback_data": f"react:{fp[:16]}:up"},
        {"text": "👎 Tidak", "callback_data": f"react:{fp[:16]}:down"},
    ])

    reply_markup = {"inline_keyboard": keyboard} if keyboard else None
    for part in chunk_text(text):
        telegram_send(session, part, reply_markup=reply_markup)


# =========================
# GOOGLE NEWS RSS
# =========================
def fetch_google_news_rss(session, query, language="id"):
    if language == "id":
        rss_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=id&gl=ID&ceid=ID:id"
    else:
        rss_url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en&gl=US&ceid=US:en"

    feed = feedparser.parse(rss_url)
    out = []
    for entry in feed.entries[:GOOGLE_RSS_SIZE]:
        pub = entry_published_utc(entry)
        out.append({
            "source": f"GoogleNews-{language.upper()}",
            "title": (entry.get("title") or "").strip(),
            "summary": (entry.get("summary") or "").strip(),
            "description": (entry.get("summary") or entry.get("description") or "").strip(),
            "url": resolve_final_url(session, entry.get("link") or ""),
            "published_utc": pub,
            "language": language,
        })
    return out


# =========================
# DIRECT RSS FEEDS
# =========================
def _matches_direct_keywords(title: str, description: str = "") -> bool:
    text = f"{title} {description}".lower()
    return any(kw in text for kw in DIRECT_RSS_KEYWORDS)


def fetch_direct_rss(session):
    all_items = []

    for feed_name, feed_url in DIRECT_RSS_FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)
            count = 0
            for entry in feed.entries[:30]:
                title = (entry.get("title") or "").strip()
                description = (entry.get("summary") or entry.get("description") or "").strip()

                if not _matches_direct_keywords(title, description):
                    continue

                pub = entry_published_utc(entry)
                link = entry.get("link") or ""

                all_items.append({
                    "source": feed_name,
                    "title": title,
                    "summary": description,
                    "description": description,
                    "url": norm_url(link),
                    "published_utc": pub,
                    "language": "id",
                })
                count += 1

            print(f"  RSS {feed_name}: {count} matched / {len(feed.entries)} total")
        except Exception as e:
            print(f"  ⚠️ RSS {feed_name} failed: {e}")

    return all_items


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

        rss_id = fetch_google_news_rss(session, QUERY_RSS_ID, language="id")
        record_source_health(con, "GoogleNews-ID", len(rss_id))

        rss_en = fetch_google_news_rss(session, QUERY_RSS_EN, language="en")
        record_source_health(con, "GoogleNews-EN", len(rss_en))

        direct_items = fetch_direct_rss(session)
        record_source_health(con, "DirectRSS", len(direct_items))

        items = rss_id + rss_en + direct_items

        # ── CHANGE 2: Apply relevance filter to ALL items ─────────────────────
        pre_filter_count = len(items)
        items = [it for it in items
                 if is_relevant(it.get("title", ""), it.get("description", ""))]
        filtered_out = pre_filter_count - len(items)
        print(f"Relevance filter: {filtered_out} dropped, {len(items)} kept "
              f"({pre_filter_count} total fetched)")
        # ─────────────────────────────────────────────────────────────────────

        # Deduplicate by fingerprint
        by_fp = {}
        for it in items:
            if not it.get("url") and not it.get("title"):
                continue
            fp = make_fingerprint(it.get("url", ""), it.get("title", ""))
            if fp not in by_fp:
                by_fp[fp] = it
            else:
                old_pub, new_pub = by_fp[fp].get("published_utc"), it.get("published_utc")
                if (old_pub is None and new_pub) or (old_pub and new_pub and new_pub > old_pub):
                    by_fp[fp] = it

        items = sorted(by_fp.values(),
                       key=lambda x: x.get("published_utc") or datetime.min.replace(tzinfo=timezone.utc),
                       reverse=True)

        pre_fuzzy = len(items)
        items = deduplicate_fuzzy(items)
        fuzzy_deduped = pre_fuzzy - len(items)

        new_items = []
        too_old = no_date = seen_skip = 0
        sent_counts = {"Positif": 0, "Negatif": 0, "Netral": 0}
        lang_counts = {"id": 0, "en": 0}

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

            description = it.get("description", "")
            sentiment = analyze_sentiment(title, description)
            tags_str = " ".join(make_hashtags(title, url))
            lang = it.get("language", "id")

            it["sentiment"] = sentiment
            sent_counts[sentiment["label"]] = sent_counts.get(sentiment["label"], 0) + 1
            lang_counts[lang] = lang_counts.get(lang, 0) + 1

            mark_seen(con, fp, url, title,
                      source=it.get("source", ""),
                      sentiment_label=sentiment["label"],
                      sentiment_score=sentiment["score"],
                      hashtags=tags_str,
                      summary=description,
                      language=lang)
            new_items.append(it)

        send_updates_batched(session, new_items)

        trending = detect_trending(con)
        send_trending_alert(session, trending, con=con)

        shift_alert = detect_sentiment_shift(con)
        if shift_alert:
            telegram_send(session, shift_alert)

        for alert in check_source_health_alerts(con):
            telegram_send(session, alert)

        if SEND_HEARTBEAT:
            sent_summary = " | ".join(
                f"{emoji} {label}: {sent_counts.get(label, 0)}"
                for label, emoji in [("Positif", "🟢"), ("Negatif", "🔴"), ("Netral", "⚪")])
            lang_summary = f"🇮🇩 {lang_counts.get('id', 0)} | 🌐 {lang_counts.get('en', 0)}"
            trend_note = f" | 🔥 Trending: {len(trending)}" if trending else ""
            dedup_note = f" | 🔁 Dedup: {fuzzy_deduped}" if fuzzy_deduped > 0 else ""
            filter_note = f" | 🚫 Filtered: {filtered_out}" if filtered_out > 0 else ""
            telegram_send(
                session,
                f"✅ BC monitor OK\n"
                f"📊 New: {len(new_items)} | Seen: {seen_skip} | Old: {too_old} | No-date: {no_date}\n"
                f"💡 Sentimen: {sent_summary}\n"
                f"🌍 Bahasa: {lang_summary}{trend_note}{dedup_note}{filter_note}\n"
                f"📡 Sources: GNews-ID {len(rss_id)} | GNews-EN {len(rss_en)} | Direct {len(direct_items)}\n"
                f"⏱️ Window: {MAX_AGE_HOURS}h")

        print(f"Done. New={len(new_items)}, seen={seen_skip}, old={too_old}, no_date={no_date}, "
              f"fetched={pre_filter_count}, filtered={filtered_out}, kept={len(items)}, "
              f"fuzzy_deduped={fuzzy_deduped}, lang={lang_counts}, trending={len(trending)}")

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
            """SELECT title, url, source, sentiment_label, hashtags, language
               FROM seen WHERE first_seen_utc >= ? ORDER BY first_seen_utc DESC""", (cutoff,))
        rows = cur.fetchall()

        if not rows:
            telegram_send(session, "📋 <b>Daily Digest</b>\n\nTidak ada artikel dalam 24 jam terakhir.")
            return

        by_sent = {"Positif": [], "Negatif": [], "Netral": []}
        id_count = en_count = 0
        for title, url, source, sent, tags, lang in rows:
            by_sent.setdefault(sent or "Netral", []).append({"title": title, "url": url, "source": source})
            if lang == "en":
                en_count += 1
            else:
                id_count += 1

        now_wib = datetime.now(timezone.utc).astimezone(WIB).strftime("%d %b %Y")
        total = len(rows)
        lines = [
            f"📋 <b>Daily Digest — {now_wib}</b>", "",
            f"📊 Total: <b>{total}</b> artikel (🇮🇩 {id_count} | 🌐 {en_count})",
            f"   🟢 Positif: {len(by_sent.get('Positif', []))}  |  🔴 Negatif: {len(by_sent.get('Negatif', []))}  |  ⚪ Netral: {len(by_sent.get('Netral', []))}",
        ]

        for label, emoji_c in [("Negatif", "🔴"), ("Positif", "🟢"), ("Netral", "⚪")]:
            items = by_sent.get(label, [])
            if not items:
                continue
            lines += ["", f"{emoji_c} <b>{label} ({len(items)}):</b>"]
            for item in items[:7]:
                t = html.escape((item["title"] or "")[:80])
                u = html.escape(item["url"] or "")
                src = html.escape((item["source"] or "")[:20])
                lines.append(f'  • <a href="{u}">{t}</a> <i>({src})</i>')
            if len(items) > 7:
                lines.append(f"  ... +{len(items) - 7} lainnya")

        for part in chunk_text("\n".join(lines), 3500):
            telegram_send(session, part)
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
# COMMAND: STATS
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

        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        wk_total = cur.fetchone()[0]
        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label", (week_cut,))
        wk_sent = dict(cur.fetchall())
        cur.execute("SELECT language, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY language", (week_cut,))
        wk_lang = dict(cur.fetchall())

        cur.execute("SELECT hashtags FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        tag_counter = Counter()
        for (t,) in cur.fetchall():
            for tag in (t or "").split():
                if tag.startswith("#"):
                    tag_counter[tag] += 1
        top_tags = tag_counter.most_common(10)

        cur.execute("SELECT source, COUNT(*) c FROM seen WHERE first_seen_utc >= ? GROUP BY source ORDER BY c DESC LIMIT 10", (week_cut,))
        top_sources = cur.fetchall()

        daily = []
        for d in range(6, -1, -1):
            ds = (now_utc - timedelta(days=d)).replace(hour=0, minute=0, second=0).isoformat()
            de = (now_utc - timedelta(days=d)).replace(hour=23, minute=59, second=59).isoformat()
            cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc <= ?", (ds, de))
            daily.append(((now_utc - timedelta(days=d)).strftime("%a"), cur.fetchone()[0]))

        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ?", (month_cut,))
        mo_total = cur.fetchone()[0]
        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label", (month_cut,))
        mo_sent = dict(cur.fetchall())

        cur.execute("SELECT source_name, consecutive_fails, total_fetches, total_articles FROM source_health")
        health_rows = cur.fetchall()

        now_wib = now_utc.astimezone(WIB).strftime("%d %b %Y %H:%M WIB")
        lines = [
            f"📊 <b>Stats Dashboard — {now_wib}</b>", "",
            "━━━ <b>Minggu Ini (7 hari)</b> ━━━",
            f"📰 Total: <b>{wk_total}</b> (🇮🇩 {wk_lang.get('id', 0)} | 🌐 {wk_lang.get('en', 0)})",
            f"🟢 {wk_sent.get('Positif', 0)}  |  🔴 {wk_sent.get('Negatif', 0)}  |  ⚪ {wk_sent.get('Netral', 0)}",
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
            f"🟢 {mo_sent.get('Positif', 0)}  |  🔴 {mo_sent.get('Negatif', 0)}  |  ⚪ {mo_sent.get('Netral', 0)}",
        ]

        if health_rows:
            lines += ["", "━━━ <b>Source Health</b> ━━━"]
            for name, fails, tf, ta in health_rows:
                status = "✅" if fails == 0 else f"⚠️ ({fails}x gagal)"
                avg = round(ta / tf, 1) if tf > 0 else 0
                lines.append(f"  {html.escape(name)}: {status} — avg {avg}/fetch")

        for part in chunk_text("\n".join(lines), 3500):
            telegram_send(session, part)
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
# COMMAND: WEEKLY LEADERBOARD
# =============================================================================
def cmd_leaderboard():
    session = build_session()
    con = sqlite3.connect(DB_FILE)
    try:
        init_db(con)
        cur = con.cursor()
        now_utc = datetime.now(timezone.utc)
        this_week_cut = (now_utc - timedelta(days=7)).isoformat()
        last_week_cut = (now_utc - timedelta(days=14)).isoformat()

        cur.execute("SELECT source, COUNT(*) c FROM seen WHERE first_seen_utc >= ? GROUP BY source ORDER BY c DESC LIMIT 10",
                    (this_week_cut,))
        src_this = cur.fetchall()

        cur.execute("SELECT source, COUNT(*) c FROM seen WHERE first_seen_utc >= ? AND first_seen_utc < ? GROUP BY source ORDER BY c DESC",
                    (last_week_cut, this_week_cut))
        src_last = dict(cur.fetchall())

        def get_tag_counts(cutoff_start, cutoff_end=None):
            if cutoff_end:
                cur.execute("SELECT hashtags FROM seen WHERE first_seen_utc >= ? AND first_seen_utc < ?",
                            (cutoff_start, cutoff_end))
            else:
                cur.execute("SELECT hashtags FROM seen WHERE first_seen_utc >= ?", (cutoff_start,))
            counter = Counter()
            for (t,) in cur.fetchall():
                for tag in (t or "").split():
                    if tag.startswith("#"):
                        counter[tag] += 1
            return counter

        tags_this = get_tag_counts(this_week_cut)
        tags_last = get_tag_counts(last_week_cut, this_week_cut)

        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label",
                    (this_week_cut,))
        sent_this = dict(cur.fetchall())
        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc < ? GROUP BY sentiment_label",
                    (last_week_cut, this_week_cut))
        sent_last = dict(cur.fetchall())

        cur.execute("SELECT language, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY language",
                    (this_week_cut,))
        lang_this = dict(cur.fetchall())

        now_wib = now_utc.astimezone(WIB).strftime("%d %b %Y")
        lines = [f"🏆 <b>Weekly Leaderboard — {now_wib}</b>", ""]

        lines.append("📰 <b>Top Sumber Berita:</b>")
        medals = ["🥇", "🥈", "🥉"]
        for i, (src, cnt) in enumerate(src_this[:10]):
            medal = medals[i] if i < 3 else f"  {i+1}."
            last_cnt = src_last.get(src, 0)
            if last_cnt == 0:
                trend = " 🆕"
            elif cnt > last_cnt:
                trend = f" ↑{cnt - last_cnt}"
            elif cnt < last_cnt:
                trend = f" ↓{last_cnt - cnt}"
            else:
                trend = " ─"
            lines.append(f"{medal} {html.escape(src)}: {cnt}{trend}")

        lines += ["", "🏷️ <b>Topik Terpanas:</b>"]
        for i, (tag, cnt) in enumerate(tags_this.most_common(10)):
            medal = medals[i] if i < 3 else f"  {i+1}."
            last_cnt = tags_last.get(tag, 0)
            if last_cnt == 0:
                trend = " 🆕"
            elif cnt > last_cnt:
                trend = f" ↑{cnt - last_cnt}"
            elif cnt < last_cnt:
                trend = f" ↓{last_cnt - cnt}"
            else:
                trend = " ─"
            lines.append(f"{medal} {html.escape(tag)}: {cnt}{trend}")

        rising = []
        for tag in tags_this:
            diff = tags_this[tag] - tags_last.get(tag, 0)
            if diff >= 2:
                rising.append((tag, diff, tags_this[tag]))
        rising.sort(key=lambda x: x[1], reverse=True)

        if rising:
            lines += ["", "📈 <b>Rising Topics:</b>"]
            for tag, diff, total in rising[:5]:
                lines.append(f"  {html.escape(tag)}: +{diff} (total: {total})")

        falling = []
        for tag in tags_last:
            diff = tags_last[tag] - tags_this.get(tag, 0)
            if diff >= 2:
                falling.append((tag, diff, tags_this.get(tag, 0)))
        falling.sort(key=lambda x: x[1], reverse=True)

        if falling:
            lines += ["", "📉 <b>Falling Topics:</b>"]
            for tag, diff, total in falling[:5]:
                lines.append(f"  {html.escape(tag)}: -{diff} (total: {total})")

        lines += ["", "📊 <b>Sentimen WoW:</b>"]
        for label, emoji in [("Positif", "🟢"), ("Negatif", "🔴"), ("Netral", "⚪")]:
            tw = sent_this.get(label, 0)
            lw = sent_last.get(label, 0)
            diff = tw - lw
            arrow = f"↑{diff}" if diff > 0 else f"↓{abs(diff)}" if diff < 0 else "─"
            lines.append(f"  {emoji} {label}: {tw} ({arrow} vs minggu lalu)")

        lines += ["", f"🌍 <b>Bahasa:</b> 🇮🇩 {lang_this.get('id', 0)} | 🌐 {lang_this.get('en', 0)}"]

        for part in chunk_text("\n".join(lines), 3500):
            telegram_send(session, part)
    except Exception as e:
        err = f"❌ Leaderboard FAILED: {type(e).__name__}: {e}"
        print(err)
        try:
            telegram_send(session, err)
        except Exception:
            pass
        raise
    finally:
        con.close()


# =============================================================================
# COMMAND: BACKFILL
# =============================================================================
def _infer_source_from_url(url: str) -> str:
    if not url:
        return ""
    domain = urlsplit(url).netloc.lower()
    domain_map = {
        "detik.com": "Detik", "finance.detik.com": "Detik-Finance",
        "kompas.com": "Kompas", "money.kompas.com": "Kompas-Ekonomi",
        "cnbcindonesia.com": "CNBC-ID", "bisnis.com": "Bisnis",
        "kontan.co.id": "Kontan", "tempo.co": "Tempo",
        "antaranews.com": "Antara", "liputan6.com": "Liputan6",
        "tribunnews.com": "Tribun", "cnnindonesia.com": "CNN-ID",
        "republika.co.id": "Republika", "mediaindonesia.com": "MediaIndonesia",
        "jawapos.com": "JawaPos", "suara.com": "Suara",
        "kumparan.com": "Kumparan", "merdeka.com": "Merdeka",
        "okezone.com": "Okezone", "sindonews.com": "SindoNews",
        "idntimes.com": "IDNTimes", "viva.co.id": "Viva",
        "reuters.com": "Reuters", "bloomberg.com": "Bloomberg",
    }
    for key, name in domain_map.items():
        if key in domain:
            return name
    parts = domain.replace("www.", "").split(".")
    return parts[0].capitalize() if parts else ""


def cmd_backfill():
    con = sqlite3.connect(DB_FILE)
    session = build_session()
    try:
        init_db(con)
        cur = con.cursor()

        cur.execute("""SELECT fingerprint, url, title, summary, source, sentiment_label, hashtags
                       FROM seen WHERE source = '' OR sentiment_label = '' OR hashtags = ''""")
        rows = cur.fetchall()

        if not rows:
            print("✅ Nothing to backfill — all articles have data.")
            telegram_send(session, "✅ Backfill: semua artikel sudah lengkap.")
            return

        updated = 0
        for fp, url, title, summary, source, sent_label, hashtags in rows:
            changes = {}

            if not source:
                inferred = _infer_source_from_url(url)
                if inferred:
                    changes["source"] = inferred

            if not sent_label:
                sentiment = analyze_sentiment(title or "", summary or "")
                changes["sentiment_label"] = sentiment["label"]
                changes["sentiment_score"] = sentiment["score"]

            if not hashtags:
                tags_str = " ".join(make_hashtags(title or "", url or ""))
                changes["hashtags"] = tags_str

            if changes:
                set_clauses = ", ".join(f"{k} = ?" for k in changes)
                values = list(changes.values()) + [fp]
                cur.execute(f"UPDATE seen SET {set_clauses} WHERE fingerprint = ?", values)
                updated += 1

        con.commit()
        print(f"✅ Backfilled {updated}/{len(rows)} articles.")
        telegram_send(session, f"✅ Backfill selesai: {updated} artikel diperbarui dari {len(rows)} yang kosong.")

    except Exception as e:
        err = f"❌ Backfill FAILED: {type(e).__name__}: {e}"
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
def cmd_export(chat_id_override=None):
    con = sqlite3.connect(DB_FILE)
    try:
        init_db(con)
        cur = con.cursor()
        cur.execute(
            """SELECT fingerprint, url, title, first_seen_utc, source,
                      sentiment_label, sentiment_score, hashtags, is_priority, summary, language
               FROM seen ORDER BY first_seen_utc DESC""")
        rows = cur.fetchall()
        with open(EXPORT_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["fingerprint", "url", "title", "first_seen_utc", "source",
                        "sentiment_label", "sentiment_score", "hashtags", "is_priority", "summary", "language"])
            w.writerows(rows)
        print(f"✅ Exported {len(rows)} articles to {EXPORT_CSV_PATH}")
        session = build_session()
        target = chat_id_override or None
        telegram_send_document(session, EXPORT_CSV_PATH,
                               caption=f"📁 CSV export: {len(rows)} artikel",
                               chat_id=target)
    except Exception as e:
        print(f"❌ Export FAILED: {type(e).__name__}: {e}")
        raise
    finally:
        con.close()


# =============================================================================
# COMMAND: PDF WEEKLY REPORT
# =============================================================================
def cmd_report():
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                     TableStyle, PageBreak)

    session = build_session()
    con = sqlite3.connect(DB_FILE)

    try:
        init_db(con)
        cur = con.cursor()
        now_utc = datetime.now(timezone.utc)
        now_wib = now_utc.astimezone(WIB)
        week_cut = (now_utc - timedelta(days=7)).isoformat()
        last_week_cut = (now_utc - timedelta(days=14)).isoformat()

        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        wk_total = cur.fetchone()[0]

        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label",
                    (week_cut,))
        wk_sent = dict(cur.fetchall())

        cur.execute("SELECT language, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY language",
                    (week_cut,))
        wk_lang = dict(cur.fetchall())

        daily = []
        for d in range(6, -1, -1):
            day = now_utc - timedelta(days=d)
            ds = day.replace(hour=0, minute=0, second=0).isoformat()
            de = day.replace(hour=23, minute=59, second=59).isoformat()
            cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc <= ?", (ds, de))
            daily.append((day.strftime("%a %d/%m"), cur.fetchone()[0]))

        cur.execute("SELECT source, COUNT(*) c FROM seen WHERE first_seen_utc >= ? GROUP BY source ORDER BY c DESC LIMIT 15",
                    (week_cut,))
        top_sources = cur.fetchall()

        cur.execute("SELECT hashtags FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        tag_counter = Counter()
        for (t,) in cur.fetchall():
            for tag in (t or "").split():
                if tag.startswith("#"):
                    tag_counter[tag] += 1
        top_tags = tag_counter.most_common(15)

        cur.execute("SELECT title, url, source, sentiment_label FROM seen WHERE first_seen_utc >= ? AND sentiment_label = 'Positif' ORDER BY first_seen_utc DESC LIMIT 10",
                    (week_cut,))
        pos_articles = cur.fetchall()

        cur.execute("SELECT title, url, source, sentiment_label FROM seen WHERE first_seen_utc >= ? AND sentiment_label = 'Negatif' ORDER BY first_seen_utc DESC LIMIT 10",
                    (week_cut,))
        neg_articles = cur.fetchall()

        cur.execute("SELECT source_name, consecutive_fails, total_fetches, total_articles FROM source_health")
        health_rows = cur.fetchall()

        doc = SimpleDocTemplate(REPORT_PDF_PATH, pagesize=A4,
                                leftMargin=20*mm, rightMargin=20*mm,
                                topMargin=20*mm, bottomMargin=20*mm)
        styles = getSampleStyleSheet()

        styles.add(ParagraphStyle("SectionHead", parent=styles["Heading2"],
                                   textColor=colors.HexColor("#1565C0"), spaceAfter=8))
        styles.add(ParagraphStyle("SmallText", parent=styles["Normal"], fontSize=8, leading=10))
        styles.add(ParagraphStyle("CellText", parent=styles["Normal"], fontSize=9, leading=11))

        story = []

        period_start = (now_utc - timedelta(days=7)).astimezone(WIB).strftime("%d %b")
        period_end = now_wib.strftime("%d %b %Y")

        story.append(Paragraph("🛃 BC News Monitor", styles["Title"]))
        story.append(Paragraph(f"Laporan Mingguan: {period_start} — {period_end}", styles["Heading3"]))
        story.append(Spacer(1, 10))

        summary_data = [
            ["Total Artikel", str(wk_total)],
            ["Bahasa Indonesia", str(wk_lang.get("id", 0))],
            ["Bahasa Inggris", str(wk_lang.get("en", 0))],
            ["Positif", str(wk_sent.get("Positif", 0))],
            ["Negatif", str(wk_sent.get("Negatif", 0))],
            ["Netral", str(wk_sent.get("Netral", 0))],
        ]
        t = Table(summary_data, colWidths=[80*mm, 40*mm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F5F5F5")),
            ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#333333")),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("FONTNAME", (1, 0), (1, -1), "Helvetica-Bold"),
        ]))
        story.append(t)
        story.append(Spacer(1, 15))

        story.append(Paragraph("Tren Harian", styles["SectionHead"]))
        daily_header = ["Hari"] + [d[0] for d in daily]
        daily_vals = ["Artikel"] + [str(d[1]) for d in daily]
        dt = Table([daily_header, daily_vals], colWidths=[50*mm] + [18*mm]*7)
        dt.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1565C0")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
            ("ALIGN", (1, 0), (-1, -1), "CENTER"),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(dt)
        story.append(Spacer(1, 15))

        story.append(Paragraph("Top Sumber Berita", styles["SectionHead"]))
        src_data = [["#", "Sumber", "Jumlah"]]
        for i, (src, cnt) in enumerate(top_sources[:10]):
            src_data.append([str(i+1), src[:50], str(cnt)])
        st = Table(src_data, colWidths=[10*mm, 100*mm, 25*mm])
        st.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1565C0")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
            ("ALIGN", (0, 0), (0, -1), "CENTER"),
            ("ALIGN", (2, 0), (2, -1), "CENTER"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8F8F8")]),
        ]))
        story.append(st)
        story.append(Spacer(1, 15))

        story.append(Paragraph("Topik Terbanyak", styles["SectionHead"]))
        tag_data = [["#", "Topik", "Jumlah"]]
        for i, (tag, cnt) in enumerate(top_tags[:10]):
            tag_data.append([str(i+1), tag, str(cnt)])
        tt = Table(tag_data, colWidths=[10*mm, 100*mm, 25*mm])
        tt.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1565C0")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
            ("ALIGN", (0, 0), (0, -1), "CENTER"),
            ("ALIGN", (2, 0), (2, -1), "CENTER"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8F8F8")]),
        ]))
        story.append(tt)

        story.append(PageBreak())

        if pos_articles:
            story.append(Paragraph("Artikel Positif (Enforcement & Capaian)", styles["SectionHead"]))
            for title, url, src, _ in pos_articles[:10]:
                story.append(Paragraph(f"<b>+</b> {title[:120]}", styles["CellText"]))
                story.append(Paragraph(f"<i>{src[:40]}</i> — <a href='{url}'>{url[:80]}</a>", styles["SmallText"]))
                story.append(Spacer(1, 4))
            story.append(Spacer(1, 10))

        if neg_articles:
            story.append(Paragraph("Artikel Negatif (Korupsi & Masalah)", styles["SectionHead"]))
            for title, url, src, _ in neg_articles[:10]:
                story.append(Paragraph(f"<b>-</b> {title[:120]}", styles["CellText"]))
                story.append(Paragraph(f"<i>{src[:40]}</i> — <a href='{url}'>{url[:80]}</a>", styles["SmallText"]))
                story.append(Spacer(1, 4))
            story.append(Spacer(1, 10))

        if health_rows:
            story.append(Paragraph("Source Health", styles["SectionHead"]))
            h_data = [["Source", "Status", "Avg/Fetch", "Total Fetch"]]
            for name, fails, tf, ta in health_rows:
                status = "OK" if fails == 0 else f"GAGAL ({fails}x)"
                avg = round(ta / tf, 1) if tf > 0 else 0
                h_data.append([name[:40], status, str(avg), str(tf)])
            ht = Table(h_data, colWidths=[60*mm, 30*mm, 25*mm, 25*mm])
            ht.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1565C0")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]))
            story.append(ht)

        story.append(Spacer(1, 20))
        story.append(Paragraph(
            f"<i>Generated: {now_wib.strftime('%d %b %Y %H:%M WIB')} — BC News Monitor v3.1</i>",
            styles["SmallText"]))

        doc.build(story)
        print(f"✅ PDF report generated: {REPORT_PDF_PATH}")

        telegram_send_document(session, REPORT_PDF_PATH,
                               caption=f"📄 Laporan Mingguan BC News — {period_start} s/d {period_end}")
        telegram_send(session, f"📄 <b>Laporan mingguan</b> telah dikirim ({wk_total} artikel, {period_start} - {period_end})")

    except Exception as e:
        err = f"❌ Report FAILED: {type(e).__name__}: {e}"
        print(err)
        try:
            telegram_send(session, err)
        except Exception:
            pass
        raise
    finally:
        con.close()


# =============================================================================
# TELEGRAM BOT COMMANDS
# =============================================================================
TELEGRAM_COMMANDS = {
    "/help": "Tampilkan daftar perintah",
    "/stats": "Statistik mingguan & bulanan",
    "/digest": "Rangkuman berita 24 jam terakhir",
    "/leaderboard": "Leaderboard sumber & topik mingguan",
    "/trending": "Topik yang sedang ramai",
    "/sentiment": "Ringkasan sentimen hari ini",
    "/mediatone": "Tone media per outlet",
    "/reactions": "Artikel paling banyak di-vote",
    "/dashboard": "Update web dashboard",
    "/export": "Export semua artikel ke CSV",
    "/report": "Buat & kirim laporan PDF mingguan",
    "/health": "Cek status sumber berita",
    "/backfill": "Isi ulang data lama (source/sentimen)",
}


def telegram_get_updates(session, offset=None):
    if not TELEGRAM_BOT_TOKEN:
        return []
    params = {"timeout": 0, "allowed_updates": '["message","callback_query"]'}
    if offset:
        params["offset"] = offset
    try:
        r = request_with_retry(session, "GET",
                               f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                               params=params, timeout=15)
        return r.json().get("result", [])
    except Exception as e:
        print(f"⚠️ getUpdates failed: {e}")
        return []


# =========================
# REACTION HANDLING
# =========================
def save_reaction(con, fingerprint_short, user_id, reaction):
    cur = con.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("SELECT fingerprint FROM seen WHERE fingerprint LIKE ?", (fingerprint_short + "%",))
    row = cur.fetchone()
    fp = row[0] if row else fingerprint_short
    cur.execute(
        "INSERT OR REPLACE INTO reactions (fingerprint, user_id, reaction, reacted_utc) VALUES (?, ?, ?, ?)",
        (fp, str(user_id), reaction, now))
    con.commit()
    return fp


def get_reaction_counts(con, fingerprint):
    cur = con.cursor()
    cur.execute("SELECT reaction, COUNT(*) FROM reactions WHERE fingerprint = ? GROUP BY reaction", (fingerprint,))
    counts = dict(cur.fetchall())
    return counts.get("up", 0), counts.get("down", 0)


def telegram_answer_callback(session, callback_query_id, text=""):
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        request_with_retry(session, "POST",
                           f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                           timeout=10,
                           json={"callback_query_id": callback_query_id, "text": text})
    except Exception:
        pass


def handle_callback_query(session, callback_query, con):
    cb_id = callback_query.get("id", "")
    data = callback_query.get("data", "")
    user = callback_query.get("from", {})
    user_id = str(user.get("id", ""))
    user_name = user.get("first_name", "User")

    if not data.startswith("react:"):
        telegram_answer_callback(session, cb_id, "❓")
        return

    parts = data.split(":")
    if len(parts) != 3 or parts[2] not in ("up", "down"):
        telegram_answer_callback(session, cb_id, "❓")
        return

    _, fp_short, reaction = parts
    fp = save_reaction(con, fp_short, user_id, reaction)
    up, down = get_reaction_counts(con, fp)

    emoji = "👍" if reaction == "up" else "👎"
    telegram_answer_callback(session, cb_id, f"{emoji} Tercatat! (👍 {up} | 👎 {down})")
    print(f"Reaction: {user_name} voted {reaction} on {fp_short} (👍{up}/👎{down})")


# =========================
# MEDIA TONE TRACKER
# =========================
def _handle_mediatone_command(session, con):
    cur = con.cursor()
    week_cut = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    cur.execute("""
        SELECT source, sentiment_label, COUNT(*)
        FROM seen WHERE first_seen_utc >= ?
        GROUP BY source, sentiment_label ORDER BY source
    """, (week_cut,))
    rows = cur.fetchall()

    if not rows:
        telegram_send(session, "📊 <b>Media Tone</b>\n\nBelum ada data minggu ini.")
        return

    sources = {}
    for src, label, cnt in rows:
        if src not in sources:
            sources[src] = {"Positif": 0, "Negatif": 0, "Netral": 0, "total": 0}
        sources[src][label] = cnt
        sources[src]["total"] += cnt

    scored = []
    for src, data in sources.items():
        if data["total"] < 2:
            continue
        tone = (data["Positif"] - data["Negatif"]) / data["total"]
        scored.append((src, tone, data))
    scored.sort(key=lambda x: x[1], reverse=True)

    lines = ["📊 <b>Media Tone Tracker (7 hari)</b>", "",
             "<i>Skor: +1.0 = selalu positif, -1.0 = selalu negatif</i>", ""]

    if scored:
        positives = [x for x in scored if x[1] > 0]
        if positives:
            lines.append("🟢 <b>Paling Positif:</b>")
            for src, tone, data in positives[:5]:
                bar = "🟩" * max(1, int(tone * 5))
                lines.append(f"  {bar} <b>{html.escape(src[:35])}</b>: {tone:+.2f}")
                lines.append(f"     {data['Positif']}+ / {data['Negatif']}- / {data['Netral']}○ ({data['total']})")

        negatives = [x for x in scored if x[1] < 0]
        if negatives:
            negatives.sort(key=lambda x: x[1])
            lines += ["", "🔴 <b>Paling Negatif:</b>"]
            for src, tone, data in negatives[:5]:
                bar = "🟥" * max(1, int(abs(tone) * 5))
                lines.append(f"  {bar} <b>{html.escape(src[:35])}</b>: {tone:+.2f}")
                lines.append(f"     {data['Positif']}+ / {data['Negatif']}- / {data['Netral']}○ ({data['total']})")

        neutrals = [x for x in scored if x[1] == 0]
        if neutrals:
            lines += ["", "⚪ <b>Netral:</b>"]
            for src, tone, data in neutrals[:3]:
                lines.append(f"  {html.escape(src[:35])}: {data['total']} artikel")

    for part in chunk_text("\n".join(lines), 3500):
        telegram_send(session, part)


# =========================
# REACTION LEADERBOARD
# =========================
def _handle_reactions_command(session, con):
    cur = con.cursor()
    cur.execute("""
        SELECT r.fingerprint, s.title, s.url, s.source,
               SUM(CASE WHEN r.reaction = 'up' THEN 1 ELSE 0 END) as ups,
               SUM(CASE WHEN r.reaction = 'down' THEN 1 ELSE 0 END) as downs,
               COUNT(*) as total_votes
        FROM reactions r LEFT JOIN seen s ON r.fingerprint = s.fingerprint
        GROUP BY r.fingerprint HAVING total_votes >= 1
        ORDER BY ups DESC, downs ASC LIMIT 15
    """)
    rows = cur.fetchall()

    if not rows:
        telegram_send(session, "👍 <b>Reactions</b>\n\nBelum ada vote. Tap 👍/👎 di artikel!")
        return

    most_relevant = [r for r in rows if r[4] > r[5]]
    least_relevant = sorted([r for r in rows if r[5] > r[4]], key=lambda x: x[5], reverse=True)

    lines = ["👍👎 <b>Article Reactions</b>", ""]

    if most_relevant:
        lines.append("🏆 <b>Paling Relevan:</b>")
        for fp, title, url, src, ups, downs, total in most_relevant[:7]:
            t = html.escape((title or "")[:70])
            u = html.escape(url or "")
            lines.append(f'  👍{ups} 👎{downs} — <a href="{u}">{t}</a>')
            lines.append(f"    <i>{html.escape((src or '')[:25])}</i>")

    if least_relevant:
        lines += ["", "👎 <b>Kurang Relevan:</b>"]
        for fp, title, url, src, ups, downs, total in least_relevant[:5]:
            t = html.escape((title or "")[:70])
            u = html.escape(url or "")
            lines.append(f'  👍{ups} 👎{downs} — <a href="{u}">{t}</a>')

    cur.execute("SELECT COUNT(DISTINCT fingerprint), COUNT(*), COUNT(DISTINCT user_id) FROM reactions")
    art_count, vote_count, voter_count = cur.fetchone()
    lines += ["", f"📊 {vote_count} votes pada {art_count} artikel dari {voter_count} voters"]

    for part in chunk_text("\n".join(lines), 3500):
        telegram_send(session, part)


def handle_bot_command(session, command, chat_id, con):
    global _reply_target_chat_id
    _reply_target_chat_id = chat_id
    try:
        cmd = command.strip().lower().split("@")[0]
        if cmd in ("/start", "/help"):
            lines = ["🛃 <b>BC News Bot v3.1 — Commands</b>\n"]
            for c, desc in TELEGRAM_COMMANDS.items():
                lines.append(f"  {c} — {desc}")
            lines.append("\n💡 Bot checks commands every 5 min.")
            telegram_send(session, "\n".join(lines))
        elif cmd == "/stats":
            cmd_stats()
        elif cmd == "/digest":
            cmd_digest()
        elif cmd == "/leaderboard":
            cmd_leaderboard()
        elif cmd == "/trending":
            _handle_trending_command(session, con)
        elif cmd == "/sentiment":
            _handle_sentiment_command(session, con)
        elif cmd == "/export":
            cmd_export(chat_id_override=chat_id)
        elif cmd == "/report":
            cmd_report()
        elif cmd == "/health":
            _handle_health_command(session, con)
        elif cmd == "/mediatone":
            _handle_mediatone_command(session, con)
        elif cmd == "/reactions":
            _handle_reactions_command(session, con)
        elif cmd == "/dashboard":
            cmd_dashboard()
        elif cmd == "/backfill":
            cmd_backfill()
        else:
            telegram_send(session, f"❓ Perintah tidak dikenal: <code>{html.escape(cmd)}</code>\nKetik /help untuk daftar.")
    finally:
        _reply_target_chat_id = None


def _handle_trending_command(session, con):
    trending = detect_trending(con)
    if not trending:
        telegram_send(session, f"📊 <b>Trending</b>\n\nTidak ada topik trending saat ini (threshold: {TRENDING_THRESHOLD} artikel dalam {TRENDING_WINDOW_HOURS} jam).")
        return
    send_trending_alert(session, trending, con=con, force=True)


def _handle_health_command(session, con):
    cur = con.cursor()
    cur.execute("SELECT source_name, consecutive_fails, total_fetches, total_articles, last_success_utc FROM source_health")
    rows = cur.fetchall()
    if not rows:
        telegram_send(session, "📡 <b>Source Health</b>\n\nBelum ada data.")
        return
    lines = ["📡 <b>Source Health</b>\n"]
    for name, fails, tf, ta, last_ok in rows:
        status = "✅ OK" if fails == 0 else f"⚠️ GAGAL ({fails}x)"
        avg = round(ta / tf, 1) if tf > 0 else 0
        lines.append(f"<b>{html.escape(name)}</b>: {status} — avg {avg}/fetch (total: {tf})")
    telegram_send(session, "\n".join(lines))


def _handle_sentiment_command(session, con):
    cur = con.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label", (cutoff,))
    counts = dict(cur.fetchall())
    total = sum(counts.values())
    lines = [
        f"📊 <b>Sentimen 24 Jam Terakhir</b>\n",
        f"Total: <b>{total}</b> artikel",
        f"🟢 Positif: {counts.get('Positif', 0)}  |  🔴 Negatif: {counts.get('Negatif', 0)}  |  ⚪ Netral: {counts.get('Netral', 0)}",
    ]
    for label, emoji, limit_n in [("Negatif", "🔴", 5), ("Positif", "🟢", 5)]:
        cur.execute("SELECT title, url FROM seen WHERE first_seen_utc >= ? AND sentiment_label = ? ORDER BY first_seen_utc DESC LIMIT ?",
                    (cutoff, label, limit_n))
        arts = cur.fetchall()
        if arts:
            lines += ["", f"{emoji} <b>{label} terbaru:</b>"]
            for title, url in arts:
                lines.append(f'  • <a href="{html.escape(url or "")}">{html.escape((title or "")[:70])}</a>')
    telegram_send(session, "\n".join(lines))


def cmd_poll():
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
        reactions = 0
        for update in updates:
            update_id = update.get("update_id", 0)

            cb = update.get("callback_query")
            if cb:
                try:
                    handle_callback_query(session, cb, con)
                    reactions += 1
                except Exception as e:
                    print(f"⚠️ Callback error: {e}")
                set_bot_state(con, "tg_update_offset", str(update_id + 1))
                continue

            msg = update.get("message", {})
            text = (msg.get("text") or "").strip()
            chat_id = str(msg.get("chat", {}).get("id", ""))
            if chat_id not in TELEGRAM_ALLOWED_CHATS:
                set_bot_state(con, "tg_update_offset", str(update_id + 1))
                continue
            if text.startswith("/"):
                print(f"Poll: '{text}' from {chat_id}")
                try:
                    handle_bot_command(session, text, chat_id, con)
                except Exception as e:
                    telegram_send(session, f"❌ Error: <code>{html.escape(str(e)[:200])}</code>")
                processed += 1
            set_bot_state(con, "tg_update_offset", str(update_id + 1))
        print(f"Poll: {len(updates)} updates, {processed} commands, {reactions} reactions.")
    except Exception as e:
        print(f"❌ Poll FAILED: {type(e).__name__}: {e}")
    finally:
        con.close()


def cmd_setup():
    session = build_session()
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN not set.")
        return
    commands = [
        {"command": "help", "description": "Tampilkan daftar perintah"},
        {"command": "stats", "description": "Statistik mingguan & bulanan"},
        {"command": "digest", "description": "Rangkuman berita 24 jam terakhir"},
        {"command": "leaderboard", "description": "Leaderboard sumber & topik"},
        {"command": "trending", "description": "Topik yang sedang ramai"},
        {"command": "sentiment", "description": "Ringkasan sentimen hari ini"},
        {"command": "mediatone", "description": "Tone media per outlet"},
        {"command": "reactions", "description": "Artikel paling banyak di-vote"},
        {"command": "dashboard", "description": "Update web dashboard"},
        {"command": "export", "description": "Export artikel ke CSV"},
        {"command": "report", "description": "Laporan PDF mingguan"},
        {"command": "health", "description": "Status sumber berita"},
        {"command": "backfill", "description": "Isi ulang data lama"},
    ]
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMyCommands"
    for scope in [None, {"type": "all_group_chats"}]:
        payload = {"commands": commands}
        if scope:
            payload["scope"] = scope
        try:
            r = request_with_retry(session, "POST", url, timeout=15, json=payload)
            data = r.json()
            scope_name = scope["type"] if scope else "default"
            print(f"✅ Commands registered ({scope_name})." if data.get("ok") else f"⚠️ {scope_name}: {data}")
        except Exception as e:
            print(f"⚠️ Setup failed: {e}")

    print("\n📋 Telegram menu registered!")
    for c in commands:
        print(f"  /{c['command']} — {c['description']}")


# =============================================================================
# COMMAND: DASHBOARD (static HTML)
# =============================================================================
DASHBOARD_HTML_PATH = "docs/index.html"


def cmd_dashboard():
    con = sqlite3.connect(DB_FILE)
    try:
        init_db(con)
        cur = con.cursor()
        now_utc = datetime.now(timezone.utc)
        now_wib = now_utc.astimezone(WIB)

        daily_data = []
        for d in range(29, -1, -1):
            day = now_utc - timedelta(days=d)
            ds = day.replace(hour=0, minute=0, second=0).isoformat()
            de = day.replace(hour=23, minute=59, second=59).isoformat()
            cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc <= ? GROUP BY sentiment_label", (ds, de))
            counts = dict(cur.fetchall())
            daily_data.append({
                "date": day.strftime("%d/%m"),
                "Positif": counts.get("Positif", 0),
                "Negatif": counts.get("Negatif", 0),
                "Netral": counts.get("Netral", 0),
            })

        cur.execute("SELECT url FROM seen WHERE first_seen_utc >= ? AND url != ''",
                    ((now_utc - timedelta(days=30)).isoformat(),))
        domain_counter = Counter()
        for (url_val,) in cur.fetchall():
            try:
                netloc = urlsplit(url_val).netloc.lower().replace("www.", "")
                if netloc:
                    domain_counter[netloc] += 1
            except Exception:
                pass
        source_data = [{"source": d, "count": c} for d, c in domain_counter.most_common(15) if d]

        week_cut = (now_utc - timedelta(days=7)).isoformat()
        cur.execute("SELECT hashtags FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        tag_counter = Counter()
        for (t,) in cur.fetchall():
            for tag in (t or "").split():
                if tag.startswith("#"):
                    tag_counter[tag] += 1
        top_10_tags = [t for t, _ in tag_counter.most_common(10)]

        heatmap_data = []
        for d in range(6, -1, -1):
            day = now_utc - timedelta(days=d)
            ds = day.replace(hour=0, minute=0, second=0).isoformat()
            de = day.replace(hour=23, minute=59, second=59).isoformat()
            cur.execute("SELECT hashtags FROM seen WHERE first_seen_utc >= ? AND first_seen_utc <= ?", (ds, de))
            day_tags = Counter()
            for (t,) in cur.fetchall():
                for tag in (t or "").split():
                    if tag in top_10_tags:
                        day_tags[tag] += 1
            row = {"date": day.strftime("%a %d/%m")}
            for tag in top_10_tags:
                row[tag] = day_tags.get(tag, 0)
            heatmap_data.append(row)

        cur.execute("SELECT url, sentiment_label FROM seen WHERE first_seen_utc >= ? AND url != '' AND sentiment_label != ''",
                    (week_cut,))
        tone_sources = {}
        for url_val, label in cur.fetchall():
            try:
                domain = urlsplit(url_val).netloc.lower().replace("www.", "")
            except Exception:
                continue
            if not domain:
                continue
            if domain not in tone_sources:
                tone_sources[domain] = {"Positif": 0, "Negatif": 0, "Netral": 0, "total": 0}
            tone_sources[domain][label] = tone_sources[domain].get(label, 0) + 1
            tone_sources[domain]["total"] += 1

        tone_data = []
        for src, data in tone_sources.items():
            if data["total"] < 2 or not src:
                continue
            tone = round((data["Positif"] - data["Negatif"]) / data["total"], 2)
            tone_data.append({"source": src, "tone": tone, **data})
        tone_data.sort(key=lambda x: x["tone"], reverse=True)
        tone_data = tone_data[:12]

        lang_daily = []
        for d in range(29, -1, -1):
            day = now_utc - timedelta(days=d)
            ds = day.replace(hour=0, minute=0, second=0).isoformat()
            de = day.replace(hour=23, minute=59, second=59).isoformat()
            cur.execute("SELECT language, COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc <= ? GROUP BY language", (ds, de))
            counts = dict(cur.fetchall())
            lang_daily.append({
                "date": day.strftime("%d/%m"),
                "id": counts.get("id", 0),
                "en": counts.get("en", 0),
            })

        month_cut = (now_utc - timedelta(days=30)).isoformat()
        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ?", (month_cut,))
        total_30d = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        total_7d = cur.fetchone()[0]
        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label", (week_cut,))
        wk_sent = dict(cur.fetchall())

        cur.execute("""
            SELECT s.title, s.url,
                   SUM(CASE WHEN r.reaction='up' THEN 1 ELSE 0 END) ups,
                   SUM(CASE WHEN r.reaction='down' THEN 1 ELSE 0 END) downs
            FROM reactions r LEFT JOIN seen s ON r.fingerprint = s.fingerprint
            GROUP BY r.fingerprint ORDER BY ups DESC LIMIT 10
        """)
        reaction_data = [{"title": t or "", "url": u or "", "ups": up, "downs": dn}
                         for t, u, up, dn in cur.fetchall()]

        last_week_cut = (now_utc - timedelta(days=14)).isoformat()
        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc < ?",
                    (last_week_cut, week_cut))
        lw_total = cur.fetchone()[0]
        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc < ? GROUP BY sentiment_label",
                    (last_week_cut, week_cut))
        lw_sent = dict(cur.fetchall())

        def _extract_domain(url_val):
            try:
                return urlsplit(url_val or "").netloc.lower().replace("www.", "") or ""
            except Exception:
                return ""

        cur.execute("SELECT title, url, source, sentiment_label FROM seen WHERE first_seen_utc >= ? AND sentiment_label = 'Positif' ORDER BY first_seen_utc DESC LIMIT 5", (week_cut,))
        top_positive = [{"title": t, "url": u, "source": _extract_domain(u) or s} for t, u, s, _ in cur.fetchall()]
        cur.execute("SELECT title, url, source, sentiment_label FROM seen WHERE first_seen_utc >= ? AND sentiment_label = 'Negatif' ORDER BY first_seen_utc DESC LIMIT 5", (week_cut,))
        top_negative = [{"title": t, "url": u, "source": _extract_domain(u) or s} for t, u, s, _ in cur.fetchall()]

        cur.execute("""SELECT title, url, source, sentiment_label, first_seen_utc, hashtags, language
                       FROM seen WHERE first_seen_utc >= ? AND title != ''
                       ORDER BY first_seen_utc DESC LIMIT 50""", (week_cut,))
        recent_articles = []
        for title, url, source, sent, seen_utc, tags, lang in cur.fetchall():
            try:
                dt = datetime.fromisoformat(seen_utc).astimezone(WIB)
                time_str = dt.strftime("%d/%m %H:%M")
            except Exception:
                time_str = ""
            try:
                display_src = urlsplit(url or "").netloc.lower().replace("www.", "") or source or ""
            except Exception:
                display_src = source or ""
            recent_articles.append({
                "title": title or "", "url": url or "", "source": display_src,
                "sentiment": sent or "Netral", "time": time_str,
                "tags": (tags or "").split()[:3], "lang": lang or "id",
            })

        dashboard_json = json.dumps({
            "generated": now_wib.strftime("%d %b %Y %H:%M WIB"),
            "summary": {
                "total_30d": total_30d,
                "total_7d": total_7d,
                "positif_7d": wk_sent.get("Positif", 0),
                "negatif_7d": wk_sent.get("Negatif", 0),
                "netral_7d": wk_sent.get("Netral", 0),
            },
            "wow": {
                "lw_total": lw_total,
                "lw_positif": lw_sent.get("Positif", 0),
                "lw_negatif": lw_sent.get("Negatif", 0),
                "lw_netral": lw_sent.get("Netral", 0),
            },
            "daily_sentiment": daily_data,
            "sources": source_data,
            "heatmap_tags": top_10_tags,
            "heatmap": heatmap_data,
            "tone": tone_data,
            "lang_daily": lang_daily,
            "reactions": reaction_data,
            "top_positive": top_positive,
            "top_negative": top_negative,
            "recent": recent_articles,
        }, ensure_ascii=False)

        html_content = _build_dashboard_html(dashboard_json)

        os.makedirs(os.path.dirname(DASHBOARD_HTML_PATH) or ".", exist_ok=True)

        nojekyll_path = os.path.join(os.path.dirname(DASHBOARD_HTML_PATH), ".nojekyll")
        if not os.path.exists(nojekyll_path):
            open(nojekyll_path, "w").close()

        with open(DASHBOARD_HTML_PATH, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"✅ Dashboard generated: {DASHBOARD_HTML_PATH}")

    except Exception as e:
        err = f"❌ Dashboard FAILED: {type(e).__name__}: {e}"
        print(err)
        raise
    finally:
        con.close()


def _build_dashboard_html(data_json: str) -> str:
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BC News Monitor — Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #0f172a; color: #e2e8f0; padding: 16px; min-height: 100vh; }}
  .container {{ max-width: 1200px; margin: 0 auto; }}
  .header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 6px; }}
  .header-icon {{ width: 44px; height: 44px; border-radius: 12px; display: flex; align-items: center;
                  justify-content: center; font-size: 24px; background: linear-gradient(135deg, #1e40af, #3b82f6); }}
  h1 {{ font-size: 1.6rem; font-weight: 700; }}
  .subtitle {{ color: #64748b; font-size: 0.85rem; margin-bottom: 20px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 14px; margin-bottom: 14px; }}
  .grid-3 {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 14px; margin-bottom: 14px; }}
  .card {{ background: #1e293b; border-radius: 14px; padding: 18px; border: 1px solid #1e3a5f; }}
  .card h2 {{ font-size: 0.85rem; color: #64748b; margin-bottom: 12px; text-transform: uppercase;
              letter-spacing: 0.5px; }}
  .stat-row {{ display: flex; gap: 16px; flex-wrap: wrap; }}
  .stat {{ flex: 1; min-width: 80px; }}
  .stat .num {{ font-size: 2rem; font-weight: 800; line-height: 1.1; }}
  .stat .label {{ font-size: 0.7rem; color: #64748b; margin-top: 2px; }}
  .stat .change {{ font-size: 0.7rem; margin-top: 2px; }}
  .up {{ color: #4ade80; }} .down {{ color: #f87171; }} .flat {{ color: #64748b; }}
  .pos {{ color: #4ade80; }} .neg {{ color: #f87171; }} .neu {{ color: #94a3b8; }}
  canvas {{ max-height: 260px; }}
  .heatmap {{ display: grid; gap: 2px; font-size: 0.65rem; }}
  .heatmap-cell {{ padding: 6px 2px; text-align: center; border-radius: 4px; min-width: 28px; font-weight: 500; }}
  .heatmap-header {{ font-weight: 600; color: #64748b; padding: 6px 2px; text-align: center; font-size: 0.65rem; }}
  .tone-bar {{ display: flex; align-items: center; margin: 6px 0; font-size: 0.8rem; gap: 8px; }}
  .tone-bar .name {{ min-width: 130px; color: #cbd5e1; font-weight: 500; white-space: nowrap; overflow: hidden;
                     text-overflow: ellipsis; }}
  .tone-bar .bar {{ height: 14px; border-radius: 7px; min-width: 4px; transition: width 0.3s; }}
  .tone-bar .val {{ color: #94a3b8; min-width: 80px; font-size: 0.75rem; }}
  .tone-pos {{ background: linear-gradient(90deg, #22c55e, #4ade80); }}
  .tone-neg {{ background: linear-gradient(90deg, #ef4444, #f87171); }}
  .article-list {{ list-style: none; }}
  .article-list li {{ padding: 8px 0; border-bottom: 1px solid #1e3a5f; display: flex; gap: 8px;
                      font-size: 0.82rem; line-height: 1.4; }}
  .article-list li:last-child {{ border-bottom: none; }}
  .article-list .dot {{ flex-shrink: 0; width: 8px; height: 8px; border-radius: 50%; margin-top: 5px; }}
  .article-list a {{ color: #93c5fd; text-decoration: none; }}
  .article-list a:hover {{ color: #60a5fa; text-decoration: underline; }}
  .article-list .src {{ color: #64748b; font-size: 0.7rem; }}
  .reaction-item {{ padding: 8px 0; border-bottom: 1px solid #1e3a5f; font-size: 0.82rem;
                    display: flex; align-items: center; gap: 8px; }}
  .reaction-item:last-child {{ border-bottom: none; }}
  .reaction-item a {{ color: #93c5fd; text-decoration: none; flex: 1; }}
  .reaction-item a:hover {{ text-decoration: underline; }}
  .reaction-votes {{ color: #94a3b8; font-size: 0.75rem; white-space: nowrap; }}
  .footer {{ text-align: center; color: #334155; font-size: 0.7rem; margin-top: 28px; padding: 12px;
             border-top: 1px solid #1e293b; }}
  .feed-controls {{ display: flex; gap: 8px; margin-bottom: 14px; flex-wrap: wrap; }}
  .feed-btn {{ background: #1e293b; border: 1px solid #1e3a5f; border-radius: 20px; color: #94a3b8;
               padding: 6px 14px; font-size: 0.75rem; cursor: pointer; font-family: inherit;
               transition: all 0.2s; }}
  .feed-btn:hover {{ border-color: #3b82f6; color: #e2e8f0; }}
  .feed-btn.active {{ background: #3b82f6; border-color: #3b82f6; color: #fff; }}
  .headline {{ padding: 12px 0; border-bottom: 1px solid #1e3a5f; }}
  .headline:last-child {{ border-bottom: none; }}
  .headline-top {{ display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }}
  .headline-dot {{ width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }}
  .headline a {{ color: #e2e8f0; text-decoration: none; font-size: 0.88rem; font-weight: 500;
                 line-height: 1.4; }}
  .headline a:hover {{ color: #93c5fd; text-decoration: underline; }}
  .headline-meta {{ display: flex; gap: 10px; font-size: 0.7rem; color: #64748b; margin-left: 16px;
                    flex-wrap: wrap; align-items: center; }}
  .headline-tag {{ background: #1e3a5f; color: #93c5fd; padding: 1px 7px; border-radius: 10px;
                   font-size: 0.65rem; }}
  .feed-empty {{ color: #334155; padding: 20px; text-align: center; }}
  .feed-count {{ color: #64748b; font-size: 0.75rem; margin-left: auto; }}
  @media (max-width: 700px) {{
    .stat .num {{ font-size: 1.5rem; }}
    .grid {{ grid-template-columns: 1fr; }}
  }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <div class="header-icon">🛃</div>
    <div><h1>BC News Monitor</h1></div>
  </div>
  <div class="subtitle" id="generated"></div>

  <div class="grid-3">
    <div class="card">
      <h2>Total Minggu Ini</h2>
      <div class="stat"><div class="num" id="total7d">-</div>
      <div class="change" id="wowTotal"></div></div>
    </div>
    <div class="card">
      <h2>Sentimen Positif</h2>
      <div class="stat"><div class="num pos" id="pos7d">-</div>
      <div class="change" id="wowPos"></div></div>
    </div>
    <div class="card">
      <h2>Sentimen Negatif</h2>
      <div class="stat"><div class="num neg" id="neg7d">-</div>
      <div class="change" id="wowNeg"></div></div>
    </div>
  </div>

  <div class="grid-3">
    <div class="card">
      <h2>Netral (7 hari)</h2>
      <div class="stat"><div class="num neu" id="neu7d">-</div></div>
    </div>
    <div class="card">
      <h2>Total 30 Hari</h2>
      <div class="stat"><div class="num" style="color:#60a5fa" id="total30d">-</div></div>
    </div>
    <div class="card">
      <h2>Rata-rata / Hari</h2>
      <div class="stat"><div class="num" style="color:#a78bfa" id="avgDaily">-</div></div>
    </div>
  </div>

  <div class="grid">
    <div class="card">
      <h2>📈 Sentimen Harian (30 hari)</h2>
      <canvas id="sentimentChart"></canvas>
    </div>
    <div class="card">
      <h2>🌍 Bahasa (30 hari)</h2>
      <canvas id="langChart"></canvas>
    </div>
  </div>

  <div class="grid">
    <div class="card">
      <h2>📰 Top Sumber Berita</h2>
      <canvas id="sourceChart"></canvas>
    </div>
    <div class="card">
      <h2>📊 Media Tone (7 hari)</h2>
      <div id="toneContainer"></div>
    </div>
  </div>

  <div class="card" style="margin-bottom:14px">
    <h2>🏷️ Topic Heatmap (7 hari)</h2>
    <div id="heatmapContainer" style="overflow-x:auto"></div>
  </div>

  <div class="card" style="margin-bottom:14px">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
      <h2 style="margin-bottom:0">📰 Berita Terbaru</h2>
      <span class="feed-count" id="feedCount"></span>
    </div>
    <div class="feed-controls">
      <button class="feed-btn active" onclick="filterFeed('all')">Semua</button>
      <button class="feed-btn" onclick="filterFeed('Positif')">🟢 Positif</button>
      <button class="feed-btn" onclick="filterFeed('Negatif')">🔴 Negatif</button>
      <button class="feed-btn" onclick="filterFeed('Netral')">⚪ Netral</button>
    </div>
    <div id="feedContainer"></div>
  </div>

  <div class="grid">
    <div class="card">
      <h2>🟢 Berita Positif Terbaru</h2>
      <ul class="article-list" id="posArticles"></ul>
    </div>
    <div class="card">
      <h2>🔴 Berita Negatif Terbaru</h2>
      <ul class="article-list" id="negArticles"></ul>
    </div>
  </div>

  <div class="card" style="margin-bottom:14px">
    <h2>👍 Top Voted Articles</h2>
    <div id="reactionsContainer"></div>
  </div>

  <div class="footer">BC News Monitor v3.1 — Auto-generated dashboard — Powered by Google News RSS</div>
</div>

<script>
const D = {data_json};

function wowArrow(curr, prev) {{
  if (prev === 0) return '<span class="flat">— baru</span>';
  const diff = curr - prev;
  const pct = ((diff / prev) * 100).toFixed(0);
  if (diff > 0) return '<span class="up">▲ +' + diff + ' (' + pct + '%)</span>';
  if (diff < 0) return '<span class="down">▼ ' + diff + ' (' + pct + '%)</span>';
  return '<span class="flat">— sama</span>';
}}

document.getElementById('generated').textContent = 'Last updated: ' + D.generated;
document.getElementById('total7d').textContent = D.summary.total_7d;
document.getElementById('total30d').textContent = D.summary.total_30d;
document.getElementById('pos7d').textContent = D.summary.positif_7d;
document.getElementById('neg7d').textContent = D.summary.negatif_7d;
document.getElementById('neu7d').textContent = D.summary.netral_7d;
document.getElementById('avgDaily').textContent = (D.summary.total_30d / 30).toFixed(1);

document.getElementById('wowTotal').innerHTML = 'vs minggu lalu: ' + wowArrow(D.summary.total_7d, D.wow.lw_total);
document.getElementById('wowPos').innerHTML = 'vs minggu lalu: ' + wowArrow(D.summary.positif_7d, D.wow.lw_positif);
document.getElementById('wowNeg').innerHTML = 'vs minggu lalu: ' + wowArrow(D.summary.negatif_7d, D.wow.lw_negatif);

Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = '#1e3a5f';
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif';

new Chart(document.getElementById('sentimentChart'), {{
  type: 'line',
  data: {{
    labels: D.daily_sentiment.map(d => d.date),
    datasets: [
      {{ label: 'Positif', data: D.daily_sentiment.map(d => d.Positif),
         borderColor: '#4ade80', backgroundColor: 'rgba(74,222,128,0.08)', fill: true, tension: 0.4, borderWidth: 2, pointRadius: 0 }},
      {{ label: 'Negatif', data: D.daily_sentiment.map(d => d.Negatif),
         borderColor: '#f87171', backgroundColor: 'rgba(248,113,113,0.08)', fill: true, tension: 0.4, borderWidth: 2, pointRadius: 0 }},
      {{ label: 'Netral', data: D.daily_sentiment.map(d => d.Netral),
         borderColor: '#64748b', backgroundColor: 'rgba(100,116,139,0.05)', fill: true, tension: 0.4, borderWidth: 1.5, pointRadius: 0 }},
    ]
  }},
  options: {{ responsive: true, interaction: {{ intersect: false, mode: 'index' }},
    plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, padding: 16 }} }} }},
    scales: {{ x: {{ ticks: {{ maxTicksLimit: 8, font: {{ size: 10 }} }} }}, y: {{ beginAtZero: true }} }} }}
}});

new Chart(document.getElementById('langChart'), {{
  type: 'bar',
  data: {{
    labels: D.lang_daily.map(d => d.date),
    datasets: [
      {{ label: '🇮🇩 Indonesia', data: D.lang_daily.map(d => d.id), backgroundColor: '#ef4444', borderRadius: 2 }},
      {{ label: '🌐 English', data: D.lang_daily.map(d => d.en), backgroundColor: '#3b82f6', borderRadius: 2 }},
    ]
  }},
  options: {{ responsive: true,
    plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, padding: 16 }} }} }},
    scales: {{ x: {{ stacked: true, ticks: {{ maxTicksLimit: 8, font: {{ size: 10 }} }} }},
               y: {{ stacked: true, beginAtZero: true }} }} }}
}});

const srcColors = ['#6366f1','#8b5cf6','#a78bfa','#c4b5fd','#818cf8','#6366f1','#7c3aed','#5b21b6','#4f46e5','#4338ca','#3730a3','#312e81'];
new Chart(document.getElementById('sourceChart'), {{
  type: 'bar',
  data: {{
    labels: D.sources.map(d => d.source.length > 28 ? d.source.slice(0,28)+'…' : d.source),
    datasets: [{{ data: D.sources.map(d => d.count),
      backgroundColor: D.sources.map((_, i) => srcColors[i % srcColors.length]),
      borderRadius: 4 }}]
  }},
  options: {{ responsive: true, indexAxis: 'y', plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ beginAtZero: true }}, y: {{ ticks: {{ font: {{ size: 11 }} }} }} }} }}
}});

const toneEl = document.getElementById('toneContainer');
if (D.tone.length) {{
  D.tone.forEach(d => {{
    const maxBar = 140;
    const barW = Math.max(6, Math.abs(d.tone) * maxBar);
    const cls = d.tone >= 0 ? 'tone-pos' : 'tone-neg';
    const sign = d.tone >= 0 ? '+' : '';
    toneEl.innerHTML += '<div class="tone-bar">' +
      '<span class="name">' + d.source.slice(0,30) + '</span>' +
      '<div class="bar ' + cls + '" style="width:' + barW + 'px"></div>' +
      '<span class="val">' + sign + d.tone.toFixed(2) + ' (' + d.total + ' art)</span></div>';
  }});
}} else {{
  toneEl.innerHTML = '<div style="color:#334155;padding:12px">Belum ada data</div>';
}}

const hmEl = document.getElementById('heatmapContainer');
if (D.heatmap_tags.length && D.heatmap.length) {{
  const cols = D.heatmap_tags.length + 1;
  let grid = '<div class="heatmap" style="grid-template-columns: 72px repeat(' + (cols-1) + ', 1fr)">';
  grid += '<div class="heatmap-header"></div>';
  D.heatmap_tags.forEach(t => {{ grid += '<div class="heatmap-header">' + t + '</div>'; }});
  D.heatmap.forEach(row => {{
    grid += '<div class="heatmap-header">' + row.date + '</div>';
    D.heatmap_tags.forEach(tag => {{
      const v = row[tag] || 0;
      const opacity = v === 0 ? 0.03 : Math.min(0.15 + v * 0.012, 0.95);
      grid += '<div class="heatmap-cell" style="background:rgba(99,102,241,' + opacity + ');' +
        (v > 0 ? 'color:#e2e8f0' : 'color:#1e293b') + '">' + (v || '') + '</div>';
    }});
  }});
  grid += '</div>';
  hmEl.innerHTML = grid;
}} else {{
  hmEl.innerHTML = '<div style="color:#334155;padding:12px">Belum ada data</div>';
}}

function renderArticles(containerId, articles, dotColor) {{
  const el = document.getElementById(containerId);
  if (!articles || !articles.length) {{
    el.innerHTML = '<li style="color:#334155;padding:8px 0">Belum ada data</li>';
    return;
  }}
  articles.forEach(a => {{
    const t = (a.title || '').slice(0, 90);
    const s = (a.source || '').slice(0, 25);
    el.innerHTML += '<li><div class="dot" style="background:' + dotColor + '"></div>' +
      '<div><a href="' + (a.url || '#') + '" target="_blank">' + t + '</a>' +
      '<div class="src">' + s + '</div></div></li>';
  }});
}}
renderArticles('posArticles', D.top_positive, '#4ade80');
renderArticles('negArticles', D.top_negative, '#f87171');

const rxEl = document.getElementById('reactionsContainer');
if (D.reactions.length) {{
  D.reactions.forEach(r => {{
    rxEl.innerHTML += '<div class="reaction-item">' +
      '<span class="reaction-votes">👍' + r.ups + ' 👎' + r.downs + '</span>' +
      '<a href="' + r.url + '" target="_blank">' + r.title.slice(0, 80) + '</a></div>';
  }});
}} else {{
  rxEl.innerHTML = '<div style="color:#334155;padding:12px">Belum ada vote — tap 👍/👎 di Telegram</div>';
}}

const sentDotColor = {{'Positif': '#4ade80', 'Negatif': '#f87171', 'Netral': '#64748b'}};
let currentFilter = 'all';

function renderFeed(filter) {{
  const el = document.getElementById('feedContainer');
  const countEl = document.getElementById('feedCount');
  const articles = filter === 'all' ? D.recent : D.recent.filter(a => a.sentiment === filter);
  countEl.textContent = articles.length + ' artikel';
  if (!articles.length) {{
    el.innerHTML = '<div class="feed-empty">Tidak ada artikel untuk filter ini</div>';
    return;
  }}
  el.innerHTML = articles.map(a => {{
    const dot = sentDotColor[a.sentiment] || '#64748b';
    const flag = a.lang === 'en' ? '🌐' : '';
    const tags = (a.tags || []).map(t => '<span class="headline-tag">' + t + '</span>').join(' ');
    return '<div class="headline">' +
      '<div class="headline-top">' +
        '<div class="headline-dot" style="background:' + dot + '"></div>' +
        '<a href="' + a.url + '" target="_blank">' + a.title.slice(0, 120) + '</a>' +
      '</div>' +
      '<div class="headline-meta">' +
        '<span>📌 ' + a.source.slice(0, 25) + '</span>' +
        '<span>🕒 ' + a.time + '</span>' +
        (flag ? '<span>' + flag + '</span>' : '') +
        tags +
      '</div>' +
    '</div>';
  }}).join('');
}}

function filterFeed(filter) {{
  currentFilter = filter;
  document.querySelectorAll('.feed-btn').forEach(b => b.classList.remove('active'));
  event.target.classList.add('active');
  renderFeed(filter);
}}

renderFeed('all');
</script>
</body>
</html>'''


# =============================================================================
# MAIN
# =============================================================================
def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    cmds = {
        "run": cmd_run,
        "digest": cmd_digest,
        "stats": cmd_stats,
        "leaderboard": cmd_leaderboard,
        "export": cmd_export,
        "report": cmd_report,
        "dashboard": cmd_dashboard,
        "backfill": cmd_backfill,
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
