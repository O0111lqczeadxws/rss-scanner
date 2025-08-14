import feedparser
import json
import os
import csv
from datetime import datetime, timezone
from dateutil.parser import parse as parse_dt
from html import unescape
from bs4 import BeautifulSoup

# Config
FEED_FILES = ["feeds.txt", "crypto_feeds.txt", "general_feeds.txt"]
CSV_PATH = "articles.csv"
JSONL_PATH = "articles.jsonl"
JSONL_MAX_ROWS = 2000
INCLUDE_KEYWORDS = []  # to be filled for VANTA2
EXCLUDE_KEYWORDS = []  # to be tuned for VANTA2

def _clean_summary(summary):
    return BeautifulSoup(unescape(summary or ""), "html.parser").get_text(" ", strip=True)

# Load existing archive
old_items = []
if os.path.exists(JSONL_PATH):
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for ln in f:
            try:
                old_items.append(json.loads(ln))
            except Exception:
                pass

# --- MIGRATE legacy records to date-only + retrieved_date ---
def _migrate_legacy_item(o):
    # published_utc -> YYYY-MM-DD
    pu = o.get("published_utc", "")
    if pu:
        if len(pu) >= 10 and pu[4] == "-" and pu[7] == "-":
            o["published_utc"] = pu[:10]
        else:
            dt = parse_dt(pu) or datetime.now(timezone.utc)
            o["published_utc"] = dt.strftime("%Y-%m-%d")
    else:
        iu = o.get("ingested_utc", "")
        o["published_utc"] = (iu[:10] if len(iu) >= 10 and iu[4] == "-" else
                              datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    # retrieved_date -> YYYY-MM-DD (new field)
    if "retrieved_date" not in o or not o.get("retrieved_date"):
        iu = o.get("ingested_utc", "")
        o["retrieved_date"] = (iu[:10] if len(iu) >= 10 and iu[4] == "-" else
                               datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    # Clean summary HTML
    o["summary"] = _clean_summary(o.get("summary", ""))

    return o

old_items = [_migrate_legacy_item(o) for o in old_items]

# Read feeds
feeds = []
for ff in FEED_FILES:
    if os.path.exists(ff):
        with open(ff, "r", encoding="utf-8") as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith("#"):
                    feeds.append(url)

# Parse feeds
new_items = []
for feed_url in feeds:
    try:
        parsed = feedparser.parse(feed_url)
        for entry in parsed.entries:
            title = entry.get("title", "").strip()
            summary = _clean_summary(entry.get("summary", ""))
            link = entry.get("link", "").strip()

            # Keyword filtering
            if INCLUDE_KEYWORDS and not any(k.lower() in (title + " " + summary).lower() for k in INCLUDE_KEYWORDS):
                continue
            if any(k.lower() in (title + " " + summary).lower() for k in EXCLUDE_KEYWORDS):
                continue

            published_raw = entry.get("published") or entry.get("updated") or ""
            if published_raw:
                dt = parse_dt(published_raw)
                published_utc = dt.astimezone(timezone.utc).strftime("%Y-%m-%d") if dt else datetime.now(timezone.utc).strftime("%Y-%m-%d")
            else:
                published_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            ingested_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            retrieved_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            item = {
                "published_utc": published_utc,
                "retrieved_date": retrieved_date,
                "source": parsed.feed.get("title", "").strip(),
                "title": title,
                "url": link,
                "id_key": hash(link),
                "ingested_utc": ingested_utc,
                "summary": summary
            }
            new_items.append(item)
    except Exception as e:
        print(f"Error fetching {feed_url}: {e}")

# Merge, sort, keep last N
all_items = old_items + new_items
all_items_sorted = sorted(
    all_items,
    key=lambda x: (x.get("published_utc", ""), x.get("ingested_utc", "")),
    reverse=True
)
all_items_sorted = all_items_sorted[:JSONL_MAX_ROWS]

# Save JSONL
with open(JSONL_PATH, "w", encoding="utf-8") as f:
    for item in all_items_sorted:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

# Save CSV with proper quoting
with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
    writer.writerow(["published_utc", "retrieved_date", "source", "title", "url", "id_key"])
    for item in all_items_sorted:
        writer.writerow([
            item.get("published_utc", ""),
            item.get("retrieved_date", ""),
            item.get("source", ""),
            item.get("title", ""),
            item.get("url", ""),
            item.get("id_key", "")
        ])
