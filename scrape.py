import os, json, hashlib, time, csv
from collections import Counter
from datetime import datetime, timezone, timedelta
from dateutil import parser as dtparse
import feedparser

# -------- Config --------
OUT_DIR = "docs"
JSONL_PATH = os.path.join(OUT_DIR, "articles.jsonl")
LATEST_PATH = os.path.join(OUT_DIR, "latest.json")
CSV_PATH   = os.path.join(OUT_DIR, "articles.csv")

# Back-compat: will still use feeds.txt if split lists aren't present
DEFAULT_FEEDS = "feeds.txt"
INCLUDE_GENERAL = os.getenv("INCLUDE_GENERAL", "false").lower() == "true"

# Retention & limits
SKIP_OLDER_DAYS = 7        # ignore items older than N days
LATEST_LIMIT    = 1000     # latest.json size
JSONL_MAX_ROWS  = 5000     # rolling archive size
PER_FEED_CAP    = 50       # max items per feed per run
SLEEP_BETWEEN_FEEDS = 1.0  # polite delay between feeds (seconds)

os.makedirs(OUT_DIR, exist_ok=True)

# -------- Feed helpers --------
def load_feeds(path):
    feeds = []
    if not os.path.exists(path):
        return feeds
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "\t" in line:
                src, url = line.split("\t", 1)
            else:
                src, url = "", line
            feeds.append((src.strip(), url.strip()))
    return feeds

def discover_feed_files():
    files = []
    # Prefer split files if present; otherwise fallback to feeds.txt
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
    if not os.path.exists(path):
        return ids
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            try:
                obj = json.loads(ln)
                k = obj.get("id_key", "")
                if k:
                    ids.add(k)
            except Exception:
                pass
    return ids

# -------- Parsing helpers --------
def parse_dt(s):
    if not s: return None
    try:
        dt = dtparse.parse(s)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def norm_item(src_name, entry):
    title = (entry.get("title") or "").strip()
    link  = (entry.get("link")  or "").strip()
    pub = (parse_dt(entry.get("published"))
           or parse_dt(entry.get("updated"))
           or parse_dt(entry.get("created"))
           or datetime.now(timezone.utc))
    summary = (entry.get("summary") or entry.get("description") or "").strip()
    src = src_name or (entry.get("source", {}) or {}).get("title", "")

    base = f"{src}|{title}|{link}|{int(pub.timestamp())}"
    id_key = hashlib.sha256(base.encode("utf-8")).hexdigest()

    return {
        "ingested_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "published_utc": pub.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": src,
        "title": title,
        "url": link,
        "summary": summary[:500],
        "lang": "",
        "id_key": id_key
    }

# -------- Main --------
def main():
    start_ts = datetime.now(timezone.utc)

    feeds = load_all_feeds()
    exist_ids = load_existing_ids(JSONL_PATH)
    cutoff = datetime.now(timezone.utc) - timedelta(days=SKIP_OLDER_DAYS)

    new_items = []
    errors = []
    by_src_counter = Counter()

    for (src, url) in feeds:
        try:
            if not url:
                continue
            d = feedparser.parse(url)
            before = len(new_items)
            for e in d.entries[:PER_FEED_CAP]:
                item = norm_item(src, e)
                pub_dt = datetime.strptime(item["published_utc"], "%Y-%m-%dT%H:%M:%SZ")
                if pub_dt < cutoff:
                    continue
                if item["id_key"] in exist_ids:
                    continue
                exist_ids.add(item["id_key"])
                new_items.append(item)
            by_src_counter[src or url] += len(new_items) - before
        except Exception as ex:
            errors.append({"source": src or url, "error": str(ex)})
        time.sleep(SLEEP_BETWEEN_FEEDS)

    # Load existing archive
    old_items = []
    if os.path.exists(JSONL_PATH):
        with open(JSONL_PATH, "r", encoding="utf-8") as f:
            for ln in f:
                try:
                    old_items.append(json.loads(ln))
                except Exception:
                    pass

    # Merge, sort, keep last N
    all_items = old_items + new_items
    all_items_sorted = sorted(all_items, key=lambda x: x["published_utc"], reverse=True)
    keep = all_items_sorted[:JSONL_MAX_ROWS]
    latest = all_items_sorted[:LATEST_LIMIT]

    # Write rolling JSONL archive
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for obj in keep:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # Write latest JSON
    with open(LATEST_PATH, "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False)

    # Write CSV
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["published_utc","source","title","url","id_key"])
        for obj in all_items_sorted:
            w.writerow([obj["published_utc"], obj.get("source",""), obj.get("title",""), obj.get("url",""), obj["id_key"]])

    # Write status.json
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
        "feed_files": discover_feed_files()
    }
    with open(os.path.join(OUT_DIR, "status.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
