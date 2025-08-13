import os, json, hashlib
from datetime import datetime, timezone, timedelta
from dateutil import parser as dtparse
import feedparser

# Output locations (GitHub Pages serves /docs)
OUT_DIR = "docs"
JSONL_PATH = os.path.join(OUT_DIR, "articles.jsonl")
LATEST_PATH = os.path.join(OUT_DIR, "latest.json")
FEEDS_PATH = "feeds.txt"

# Simple retention & limits
SKIP_OLDER_DAYS = 7       # ignore items older than N days
LATEST_LIMIT = 1000       # latest.json size
JSONL_MAX_ROWS = 5000     # rolling archive size

os.makedirs(OUT_DIR, exist_ok=True)

def load_feeds(path):
    feeds = []
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

def load_existing_ids(path):
    ids = set()
    if not os.path.exists(path):
        return ids
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            try:
                obj = json.loads(ln)
                ids.add(obj.get("id_key", ""))
            except Exception:
                pass
    return ids

def parse_dt(s):
    if not s:
        return None
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

    # Try several date fields
    pub = parse_dt(entry.get("published")) or \
          parse_dt(entry.get("updated")) or \
          parse_dt(entry.get("created"))
    if not pub:
        pub = datetime.now(timezone.utc)

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
        "lang": "",   # placeholder (add language detection later if wanted)
        "id_key": id_key
    }

def main():
    feeds = load_feeds(FEEDS_PATH)
    exist_ids = load_existing_ids(JSONL_PATH)
    cutoff = datetime.now(timezone.utc) - timedelta(days=SKIP_OLDER_DAYS)

    new_items = []
    for (src, url) in feeds:
        d = feedparser.parse(url)
        for e in d.entries[:50]:  # per-feed cap per run
            item = norm_item(src, e)
            pub_dt = datetime.strptime(item["published_utc"], "%Y-%m-%dT%H:%M:%SZ")
            if pub_dt < cutoff:
                continue
            if item["id_key"] in exist_ids:
                continue
            exist_ids.add(item["id_key"])
            new_items.append(item)

    # Load existing archive
    old_items = []
    if os.path.exists(JSONL_PATH):
        with open(JSONL_PATH, "r", encoding="utf-8") as f:
            for ln in f:
                try:
                    old_items.append(json.loads(ln))
                except Exception:
                    pass

    # Merge, sort, and keep last N
    all_items = old_items + new_items
    all_items_sorted = sorted(all_items, key=lambda x: x["published_utc"], reverse=True)
    keep = all_items_sorted[:JSONL_MAX_ROWS]

    # Write rolling JSONL archive
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for obj in keep:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # Write latest.json for easy consumption (Sheets, etc.)
    latest = all_items_sorted[:LATEST_LIMIT]
    with open(LATEST_PATH, "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False)

if __name__ == "__main__":
    main()
