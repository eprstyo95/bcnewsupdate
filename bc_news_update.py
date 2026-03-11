#!/usr/bin/env python3
"""
BC News Monitor v3.0
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

# Indonesian queries
QUERY_RSS_ID = 'bea cukai OR DJBC OR Kemenkeu OR "Kementerian Keuangan" when:24h'

# English queries (international coverage)
QUERY_RSS_EN = '("Indonesia customs" OR "Indonesia tariff" OR "DGCE Indonesia" OR "Indonesia trade policy" OR "Indonesia import export") when:24h'

MAX_AGE_HOURS = 24

GOOGLE_RSS_SIZE = 30

MAX_ITEMS_PER_BATCH = 1
SEND_HEARTBEAT = True

INCLUDE_SNIPPET = True
SNIPPET_MAX_CHARS = 150

WIB = timezone(timedelta(hours=7))

# Trending: alert if a hashtag appears >= this many times in TRENDING_WINDOW_HOURS
TRENDING_THRESHOLD = 4
TRENDING_WINDOW_HOURS = 3

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
    """Init DB with v3 schema: seen + source_health + bot_state tables."""
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

    # bot_state table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
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
    cur = con.cursor()
    cur.execute("SELECT source_name, consecutive_fails, last_success_utc FROM source_health WHERE consecutive_fails >= 3")
    return [
        f"⚠️ <b>{html.escape(name)}</b> returned 0 articles {fails}x in a row. Last OK: {html.escape(last_ok or 'never')}"
        for name, fails, last_ok in cur.fetchall()
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
        # English-specific
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
    """Check if any hashtag has spiked in the last TRENDING_WINDOW_HOURS."""
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


def send_trending_alert(session, trending_topics):
    """Send a trending alert if any topics are spiking."""
    if not trending_topics:
        return

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

    text = "\n".join(lines)
    buttons = [{"text": "📖 Baca Artikel", "url": url}] if url else []
    if title:
        search_url = f"https://www.google.com/search?q={quote(title[:80])}"
        buttons.append({"text": "🔍 Cari Lebih", "url": search_url})

    reply_markup = build_inline_keyboard(buttons) if buttons else None
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

        # Fetch Indonesian sources
        rss_id = fetch_google_news_rss(session, QUERY_RSS_ID, language="id")
        record_source_health(con, "GoogleNews-ID", len(rss_id))

        # Fetch English sources
        rss_en = fetch_google_news_rss(session, QUERY_RSS_EN, language="en")
        record_source_health(con, "GoogleNews-EN", len(rss_en))

        items = rss_id + rss_en

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

        # Trending detection
        trending = detect_trending(con)
        send_trending_alert(session, trending)

        # Source health alerts
        for alert in check_source_health_alerts(con):
            telegram_send(session, alert)

        # Heartbeat
        if SEND_HEARTBEAT:
            sent_summary = " | ".join(
                f"{emoji} {label}: {sent_counts.get(label, 0)}"
                for label, emoji in [("Positif", "🟢"), ("Negatif", "🔴"), ("Netral", "⚪")])
            lang_summary = f"🇮🇩 {lang_counts.get('id', 0)} | 🌐 {lang_counts.get('en', 0)}"
            trend_note = f" | 🔥 Trending: {len(trending)}" if trending else ""
            telegram_send(
                session,
                f"✅ BC monitor OK\n"
                f"📊 New: {len(new_items)} | Seen: {seen_skip} | Old: {too_old} | No-date: {no_date}\n"
                f"💡 Sentimen: {sent_summary}\n"
                f"🌍 Bahasa: {lang_summary}{trend_note}\n"
                f"⏱️ Window: {MAX_AGE_HOURS}h")

        print(f"Done. New={len(new_items)}, seen={seen_skip}, old={too_old}, no_date={no_date}, "
              f"fetched={len(items)}, lang={lang_counts}, trending={len(trending)}")

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

        # Top hashtags
        cur.execute("SELECT hashtags FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        tag_counter = Counter()
        for (t,) in cur.fetchall():
            for tag in (t or "").split():
                if tag.startswith("#"):
                    tag_counter[tag] += 1
        top_tags = tag_counter.most_common(10)

        # Top sources
        cur.execute("SELECT source, COUNT(*) c FROM seen WHERE first_seen_utc >= ? GROUP BY source ORDER BY c DESC LIMIT 10", (week_cut,))
        top_sources = cur.fetchall()

        # Daily trend
        daily = []
        for d in range(6, -1, -1):
            ds = (now_utc - timedelta(days=d)).replace(hour=0, minute=0, second=0).isoformat()
            de = (now_utc - timedelta(days=d)).replace(hour=23, minute=59, second=59).isoformat()
            cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc <= ?", (ds, de))
            daily.append(((now_utc - timedelta(days=d)).strftime("%a"), cur.fetchone()[0]))

        # Monthly
        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ?", (month_cut,))
        mo_total = cur.fetchone()[0]
        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label", (month_cut,))
        mo_sent = dict(cur.fetchall())

        # Health
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

        # --- Source leaderboard ---
        cur.execute("SELECT source, COUNT(*) c FROM seen WHERE first_seen_utc >= ? GROUP BY source ORDER BY c DESC LIMIT 10",
                    (this_week_cut,))
        src_this = cur.fetchall()

        cur.execute("SELECT source, COUNT(*) c FROM seen WHERE first_seen_utc >= ? AND first_seen_utc < ? GROUP BY source ORDER BY c DESC",
                    (last_week_cut, this_week_cut))
        src_last = dict(cur.fetchall())

        # --- Topic leaderboard ---
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

        # --- Sentiment trend ---
        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label",
                    (this_week_cut,))
        sent_this = dict(cur.fetchall())
        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc < ? GROUP BY sentiment_label",
                    (last_week_cut, this_week_cut))
        sent_last = dict(cur.fetchall())

        # --- Language breakdown ---
        cur.execute("SELECT language, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY language",
                    (this_week_cut,))
        lang_this = dict(cur.fetchall())

        # Build message
        now_wib = now_utc.astimezone(WIB).strftime("%d %b %Y")
        lines = [f"🏆 <b>Weekly Leaderboard — {now_wib}</b>", ""]

        # Source rankings
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

        # Topic rankings with trend
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

        # Rising topics (biggest increase)
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

        # Falling topics
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

        # Sentiment trend
        lines += ["", "📊 <b>Sentimen WoW:</b>"]
        for label, emoji in [("Positif", "🟢"), ("Negatif", "🔴"), ("Netral", "⚪")]:
            tw = sent_this.get(label, 0)
            lw = sent_last.get(label, 0)
            diff = tw - lw
            arrow = f"↑{diff}" if diff > 0 else f"↓{abs(diff)}" if diff < 0 else "─"
            lines.append(f"  {emoji} {label}: {tw} ({arrow} vs minggu lalu)")

        # Language
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
    """Generate a PDF weekly report and send via Telegram."""
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

        # Gather data
        cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        wk_total = cur.fetchone()[0]

        cur.execute("SELECT sentiment_label, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY sentiment_label",
                    (week_cut,))
        wk_sent = dict(cur.fetchall())

        cur.execute("SELECT language, COUNT(*) FROM seen WHERE first_seen_utc >= ? GROUP BY language",
                    (week_cut,))
        wk_lang = dict(cur.fetchall())

        # Daily counts
        daily = []
        for d in range(6, -1, -1):
            day = now_utc - timedelta(days=d)
            ds = day.replace(hour=0, minute=0, second=0).isoformat()
            de = day.replace(hour=23, minute=59, second=59).isoformat()
            cur.execute("SELECT COUNT(*) FROM seen WHERE first_seen_utc >= ? AND first_seen_utc <= ?", (ds, de))
            daily.append((day.strftime("%a %d/%m"), cur.fetchone()[0]))

        # Top sources
        cur.execute("SELECT source, COUNT(*) c FROM seen WHERE first_seen_utc >= ? GROUP BY source ORDER BY c DESC LIMIT 15",
                    (week_cut,))
        top_sources = cur.fetchall()

        # Top tags
        cur.execute("SELECT hashtags FROM seen WHERE first_seen_utc >= ?", (week_cut,))
        tag_counter = Counter()
        for (t,) in cur.fetchall():
            for tag in (t or "").split():
                if tag.startswith("#"):
                    tag_counter[tag] += 1
        top_tags = tag_counter.most_common(15)

        # Top articles by sentiment
        cur.execute("SELECT title, url, source, sentiment_label FROM seen WHERE first_seen_utc >= ? AND sentiment_label = 'Positif' ORDER BY first_seen_utc DESC LIMIT 10",
                    (week_cut,))
        pos_articles = cur.fetchall()

        cur.execute("SELECT title, url, source, sentiment_label FROM seen WHERE first_seen_utc >= ? AND sentiment_label = 'Negatif' ORDER BY first_seen_utc DESC LIMIT 10",
                    (week_cut,))
        neg_articles = cur.fetchall()

        # Source health
        cur.execute("SELECT source_name, consecutive_fails, total_fetches, total_articles FROM source_health")
        health_rows = cur.fetchall()

        # ─── Build PDF ───
        doc = SimpleDocTemplate(REPORT_PDF_PATH, pagesize=A4,
                                leftMargin=20*mm, rightMargin=20*mm,
                                topMargin=20*mm, bottomMargin=20*mm)
        styles = getSampleStyleSheet()

        # Custom styles
        styles.add(ParagraphStyle("SectionHead", parent=styles["Heading2"],
                                   textColor=colors.HexColor("#1565C0"), spaceAfter=8))
        styles.add(ParagraphStyle("SmallText", parent=styles["Normal"], fontSize=8, leading=10))
        styles.add(ParagraphStyle("CellText", parent=styles["Normal"], fontSize=9, leading=11))

        story = []

        # Title
        period_start = (now_utc - timedelta(days=7)).astimezone(WIB).strftime("%d %b")
        period_end = now_wib.strftime("%d %b %Y")

        story.append(Paragraph("🛃 BC News Monitor", styles["Title"]))
        story.append(Paragraph(f"Laporan Mingguan: {period_start} — {period_end}", styles["Heading3"]))
        story.append(Spacer(1, 10))

        # Summary box
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

        # Daily trend table
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

        # Top sources table
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

        # Top topics table
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

        # Page break for articles
        story.append(PageBreak())

        # Positive articles
        if pos_articles:
            story.append(Paragraph("Artikel Positif (Enforcement & Capaian)", styles["SectionHead"]))
            for title, url, src, _ in pos_articles[:10]:
                story.append(Paragraph(f"<b>+</b> {title[:120]}", styles["CellText"]))
                story.append(Paragraph(f"<i>{src[:40]}</i> — <a href='{url}'>{url[:80]}</a>", styles["SmallText"]))
                story.append(Spacer(1, 4))
            story.append(Spacer(1, 10))

        # Negative articles
        if neg_articles:
            story.append(Paragraph("Artikel Negatif (Korupsi & Masalah)", styles["SectionHead"]))
            for title, url, src, _ in neg_articles[:10]:
                story.append(Paragraph(f"<b>-</b> {title[:120]}", styles["CellText"]))
                story.append(Paragraph(f"<i>{src[:40]}</i> — <a href='{url}'>{url[:80]}</a>", styles["SmallText"]))
                story.append(Spacer(1, 4))
            story.append(Spacer(1, 10))

        # Source health
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

        # Footer
        story.append(Spacer(1, 20))
        story.append(Paragraph(
            f"<i>Generated: {now_wib.strftime('%d %b %Y %H:%M WIB')} — BC News Monitor v3.0</i>",
            styles["SmallText"]))

        doc.build(story)
        print(f"✅ PDF report generated: {REPORT_PDF_PATH}")

        # Send via Telegram
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
    "/export": "Export semua artikel ke CSV",
    "/report": "Buat & kirim laporan PDF mingguan",
    "/health": "Cek status sumber berita",
}


def telegram_get_updates(session, offset=None):
    if not TELEGRAM_BOT_TOKEN:
        return []
    params = {"timeout": 0, "allowed_updates": '["message"]'}
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


def handle_bot_command(session, command, chat_id, con):
    global _reply_target_chat_id
    _reply_target_chat_id = chat_id
    try:
        cmd = command.strip().lower().split("@")[0]
        if cmd in ("/start", "/help"):
            lines = ["🛃 <b>BC News Bot v3 — Commands</b>\n"]
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
        else:
            telegram_send(session, f"❓ Perintah tidak dikenal: <code>{html.escape(cmd)}</code>\nKetik /help untuk daftar.")
    finally:
        _reply_target_chat_id = None


def _handle_trending_command(session, con):
    trending = detect_trending(con)
    if not trending:
        telegram_send(session, f"📊 <b>Trending</b>\n\nTidak ada topik trending saat ini (threshold: {TRENDING_THRESHOLD} artikel dalam {TRENDING_WINDOW_HOURS} jam).")
        return
    send_trending_alert(session, trending)


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
        for update in updates:
            update_id = update.get("update_id", 0)
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
        print(f"Poll: {len(updates)} updates, {processed} commands.")
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
        {"command": "export", "description": "Export artikel ke CSV"},
        {"command": "report", "description": "Laporan PDF mingguan"},
        {"command": "health", "description": "Status sumber berita"},
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
