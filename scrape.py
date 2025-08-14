import os, json, hashlib, time, csv, re, html
import urllib.request, urllib.error
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from collections import Counter
from datetime import datetime, timezone, timedelta
from dateutil import parser as dtparse
import feedparser

# ---------- Config ----------
OUT_DIR = "docs"
JSONL_PATH = os.path.join(OUT_DIR, "articles.jsonl")
LATEST_PATH = os.path.join(OUT_DIR, "latest.json")
CSV_PATH   = os.path.join(OUT_DIR, "articles.csv")

DEFAULT_FEEDS = "feeds.txt"  # fallback if split files absent
INCLUDE_GENERAL = os.getenv("INCLUDE_GENERAL", "false").lower() == "true"

# Freshness & limits
SKIP_OLDER_DAYS = 10
LATEST_LIMIT    = 1000
JSONL_MAX_ROWS  = 5000
PER_FEED_CAP    = 50
LATEST_PER_SOURCE_CAP = 200
SLEEP_BETWEEN_FEEDS = 1.0  # seconds

# Keyword filters (case-insensitive)
KEYWORDS_INCLUDE = [
    "bitcoin","btc","ethereum","eth","etf","sec","staking","solana",
    "layer 2","airdrop","wallet","custody","treasury","mining","stablecoin"
]
KEYWORDS_EXCLUDE = ["casino","giveaway","price prediction","sponsored","press release"]

# ---------- Derived / helpers ----------
_rx_inc = re.compile("|".join([re.escape(k) for k in KEYWORDS_INCLUDE]), re.I) if KEYWORDS_INCLUDE else None
_rx_exc = re.compile("|".join([re.escape(k) for k in KEYWORDS_EXCLUDE]), re.I) if KEYWORDS_EXCLUDE else None

def _passes_keywords(title, summary):
    text = f"{title or ''} {summary or ''}".lower()
    if _rx_exc and _rx_exc.search(text): return False
    if _rx_inc and not _rx_inc.search(text): return False
    return True

def _dedupe_key(title, link):
    t = (title or "").strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t or (link or "").lower()

def _normalize_url(u: str) -> str:
    if not u:
        return ""
    try:
        p = urlparse(u)
        # drop utm_* and fragment
        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q), ""))
    except Exception:
        return u

def _clean_summary(s: str) -> str:
    if not s: return ""
    s = re.sub(r"<[^>]+>", " ", s)   # remove HTML tags
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()

os.makedirs(OUT_DIR, exist_ok=True)

# ---------- Feed helpers ----------
def load_feeds(path):
    feeds = []
    if not os.path.exists(path): return feeds
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            if "\t" in line:
                src, url = line.split("\t", 1)
            else:
                parts = line.split()
                if len(parts) >= 2 and parts[1].startswith("http"):
                    src, url = parts[0], " ".join(parts[1:])
                else:
                    src, url = "", line
            feeds.append((src.strip(), url.strip()))
    return feeds

def discover_feed_files():
    files = []
    if os.path.exists("crypto_feeds.txt"):
        files.append("crypto_feeds.txt")
    if INCLUDE_GENERAL and os.path.exists("general_feeds.txt"):
        files.append("general_feeds.txt")
    if not files and os.path.exists(DEFAULT_FEEDS):
        files.append(DEFAULT_FEEDS)
    return files

def load_all_feeds():
    merged = []
    for p in discover_feed_files():
        merged.extend(load_feeds(p))
    return merged

def load_existing_ids(path):
    ids = set()
    if not os.path.exists(path): return ids
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            try:
                obj = json.loads(ln)
                k = obj.get("id_key", "")
                if k: ids.add(k)
            except Exception:
                pass
    return ids

# ---------- Networking (robust fetch with UA) ----------
UA = "Mozilla/5.0 (compatible; VANTA-RSS/1.0; +https://example.com)"

def fetch_bytes(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def parse_feed(url):
    data = fetch_bytes(url)
    return feedparser.parse(data)

# ---------- Parsing helpers ----------
def parse_dt(s):
    if not s: return None
    try:
        dt = dtparse.parse(s)
        if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def norm_item(src_name, entry):
    title = (entry.get("title") or "").strip()
    link  = _normalize_url((entry.get("link") or "").strip())

    # Resolve published datetime (UTC); fall back to "now" if missing
    pub = (parse_dt(entry.get("published"))
           or parse_dt(entry.get("updated"))
           or parse_dt(entry.get("created"))
           or datetime.now(timezone.utc))

    # --- date-only strings (UTC) ---
    published_date = pub.strftime("%Y-%m-%d")
    retrieved_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    summary = _clean_summary(entry.get("summary") or entry.get("description") or "")
    src = src_name or (entry.get("source", {}) or {}).get("title", "")

    # id_key based on stable tuple
    base = f"{src}|{title}|{link}|{int(pub.timestamp())}"
    id_key = hashlib.sha256(base.encode("utf-8")).hexdigest()

    return {
        "ingested_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "published_utc": published_date,     # YYYY-MM-DD
        "retrieved_date": retrieved_date,    # YYYY-MM-DD
        "source": src,
        "title": title,
        "url": link,
        "summary": summary[:500],
        "lang": "",
        "id_key": id_key
    }

# ---------- Main ----------
def main():
    start_ts = datetime.now(timezone.utc)

    feeds = load_all_feeds()
    exist_ids = load_existing_ids(JSONL_PATH)
    cutoff = datetime.now(timezone.utc) - timedelta(days=SKIP_OLDER_DAYS)

    new_items, errors = [], []
    by_src_counter = Counter()
    seen_titles = set()

    stats = {
        "feeds_total": len(feeds),
        "feeds_error": 0,
        "entries_seen": 0,
        "too_old": 0,
        "filtered_exclude": 0,
        "filtered_include_miss": 0,
        "dup_title": 0,
        "dup_id": 0
    }

    for (src, url) in feeds:
        try:
            if not url: continue
            d = parse_feed(url)
            if getattr(d, "bozo", 0):
                errors.append({"source": src or url, "error": str(getattr(d, "bozo_exception", ""))})
            before = len(new_items)
            for e in d.entries[:PER_FEED_CAP]:
                stats["entries_seen"] += 1
                item = norm_item(src, e)

                # Parse date-only published_utc back to a datetime for cutoff comparison
                try:
                    pub_dt = dtparse.isoparse(item["published_utc"])  # YYYY-MM-DD
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                    else:
                        pub_dt = pub_dt.astimezone(timezone.utc)
                    # Ensure midnight UTC for date-only
                    pub_dt = pub_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
                except Exception:
                    pub_dt = datetime.now(timezone.utc)

                if pub_dt < cutoff:
                    stats["too_old"] += 1
                    continue

                if not _passes_keywords(item["title"], item["summary"]):
                    txt = f"{item['title']} {item['summary']}"
                    if _rx_exc and _rx_exc.search(txt):
                        stats["filtered_exclude"] += 1
                    elif _rx_inc and not _rx_inc.search(txt):
                        stats["filtered_include_miss"] += 1
                    continue

                dk = _dedupe_key(item["title"], item["url"])
                if dk in seen_titles:
                    stats["dup_title"] += 1
                    continue
                seen_titles.add(dk)

                if item["id_key"] in exist_ids:
                    stats["dup_id"] += 1
                    continue
                exist_ids.add(item["id_key"])

                new_items.append(item)
            by_src_counter[src or url] += len(new_items) - before
        except (urllib.error.URLError, urllib.error.HTTPError) as net_ex:
            stats["feeds_error"] += 1
            errors.append({"source": src or url, "error": f"net: {net_ex}"})
        except Exception as ex:
            stats["feeds_error"] += 1
            errors.append({"source": src or url, "error": str(ex)})
        time.sleep(SLEEP_BETWEEN_FEEDS)

    # Load existing archive
    old_items = []
    if os.path.exists(JSONL_PATH):
        with open(JSONL_PATH, "r", encoding="utf-8") as f:
            for ln in f:
                try: old_items.append(json.loads(ln))
                except Exception: pass

    # Merge, sort, keep last N
    all_items = old_items + new_items
    # Sort primarily by published date (string YYYY-MM-DD), tiebreak by ingested_utc
    all_items_sorted = sorted(
        all_items,
        key=lambda x: (x.get("published_utc",""), x.get("ingested_utc","")),
        reverse=True
    )
    keep = all_items_sorted[:JSONL_MAX_ROWS]

    latest = all_items_sorted[:LATEST_LIMIT]
    # Per-source cap in latest
    capped, seen = [], Counter()
    for it in latest:
        s = (it.get("source") or "").strip()
        if seen[s] < LATEST_PER_SOURCE_CAP:
            capped.append(it)
            seen[s] += 1
    latest = capped

    # Write outputs
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for obj in keep:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    with open(LATEST_PATH, "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False)

    hot = [o for o in latest if _passes_keywords(o.get("title",""), o.get("summary",""))]
    with open(os.path.join(OUT_DIR, "hot.json"), "w", encoding="utf-8") as f:
        json.dump(hot, f, ensure_ascii=False)

    # CSV now includes retrieved_date and date-only published_utc
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["published_utc","retrieved_date","source","title","url","id_key"])
        for obj in all_items_sorted:
            w.writerow([
                obj.get("published_utc",""),     # YYYY-MM-DD
                obj.get("retrieved_date",""),    # YYYY-MM-DD
                obj.get("source",""),
                obj.get("title",""),
                obj.get("url",""),
                obj.get("id_key","")
            ])

    archive_dir = os.path.join(OUT_DIR, "archive")
    os.makedirs(archive_dir, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap = os.path.join(archive_dir, f"{day}.jsonl")
    if new_items:
        with open(snap, "a", encoding="utf-8") as f:
            for obj in new_items:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    end_ts = datetime.now(timezone.utc)
    status = {
        "started_utc": start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ended_utc": end_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_ms": int((end_ts - start_ts).total_seconds() * 1000),
        "new_items_this_run": len(new_items),
        "total_in_latest": len(latest),
        "by_source": dict(by_src_counter),
        "errors": errors,
        "include_general": INCLUDE_GENERAL,
        "feed_files": discover_feed_files(),
        "filters": {"include": KEYWORDS_INCLUDE, "exclude": KEYWORDS_EXCLUDE},
        "limits": {
            "skip_older_days": SKIP_OLDER_DAYS,
            "latest_limit": LATEST_LIMIT,
            "latest_per_source_cap": LATEST_PER_SOURCE_CAP
        },
        "stats": stats
    }
    with open(os.path.join(OUT_DIR, "status.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
