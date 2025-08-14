import feedparser
import json
from datetime import datetime, timezone
from pathlib import Path

# Output directory
DATA_DIR = Path("data/processed")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Output file for today
DATE_STR = datetime.now(timezone.utc).date().isoformat()
JSONL_PATH = DATA_DIR / f"{DATE_STR}.jsonl"

# Feeds list
GENERAL_FEEDS_FILE = Path("general_feeds.txt")
FEEDS = []

if GENERAL_FEEDS_FILE.exists():
    with open(GENERAL_FEEDS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if url and not url.startswith("#"):
                FEEDS.append(url)

def fetch_feed(url):
    """Fetch a single RSS/Atom feed."""
    try:
        parsed = feedparser.parse(url)
        return parsed.entries
    except Exception as e:
        print(f"[error] Failed to fetch {url}: {e}")
        return []

def load_existing():
    """Load existing JSONL entries to avoid duplicates."""
    if not JSONL_PATH.exists():
        return set()
    seen = set()
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                seen.add(item.get("url"))
            except json.JSONDecodeError:
                continue
    return seen

def save_entries(entries):
    """Save new entries to the JSONL file."""
    if not entries:
        return
    with open(JSONL_PATH, "a", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def process_entry(entry, source):
    """Normalize feed entry into a dict."""
    return {
        "source": source,
        "title": entry.get("title", "").strip(),
        "url": entry.get("link", "").strip(),
        "published_utc": entry.get("published", ""),
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "keywords": [],
        "summary": entry.get("summary", "").strip()
    }

def main():
    seen_urls = load_existing()
    new_entries = []
    for feed_url in FEEDS:
        entries = fetch_feed(feed_url)
        for e in entries:
            source = feed_url.split("/")[2]  # crude source name from URL
            item = process_entry(e, source)
            if item["url"] and item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                new_entries.append(item)

    if new_entries:
        save_entries(new_entries)
        print(f"[rss] Saved {len(new_entries)} new entries to {JSONL_PATH}")
    else:
        print("[rss] No new entries.")

if __name__ == "__main__":
    main()
