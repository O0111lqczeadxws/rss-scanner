import os, json, hashlib, time, csv, re, html
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from datetime import datetime, timezone
from dateutil import parser as dtparse
import feedparser

# ---------- Config ----------
OUT_DIR = "docs"
JSONL_PATH = os.path.join(OUT_DIR, "articles.jsonl")
LATEST_PATH = os.path.join(OUT_DIR, "latest.json")
CSV_PATH = os.path.join(OUT_DIR, "articles.csv")

SKIP_OLDER_DAYS = 10
LATEST_LIMIT = 1000
JSONL_MAX_ROWS = 5000
PER_FEED_CAP = 50
LATEST_PER_SOURCE_CAP = 200
SLEEP_BETWEEN_FEEDS = 1.0

KEYWORDS_INCLUDE = [
    "bitcoin", "btc", "ethereum", "eth", "etf", "sec", "staking", "solana",
    "layer 2", "airdrop", "wallet", "custody", "treasury", "mining", "stablecoin"
]
KEYWORDS_EXCLUDE = ["casino", "giveaway", "price prediction", "sponsored", "press release"]

ALLOWLIST_DOMAINS = {
    "sec.gov", "federalreserve.gov", "home.treasury.gov", "bls.gov",
    "bankofcanada.ca", "ecb.europa.eu", "fca.org.uk", "esma.europa.eu",
    "statcan.gc.ca", "www150.statcan.gc.ca", "iea.org", "opec.org",
    "cmegroup.com", "reuters.com", "feeds.reuters.com"
}

_rx_inc = re.compile("|".join([re.escape(k) for k in KEYWORDS_INCLUDE]), re.I) if KEYWORDS_INCLUDE else None
_rx_exc = re.compile("|".join([re.escape(k) for k in KEYWORDS_EXCLUDE]), re.I) if KEYWORDS_EXCLUDE else None

# ---------- Helpers ----------
def _passes_keywords(title, summary):
    text = f"{title or ''} {summary or ''}".lower()
    if _rx_exc and _rx_exc.search(text): return False
    if _rx_inc and not _rx_inc.search(text): return False
    return True

def _normalize_url(u):
    if not u: return ""
    try:
        p = urlparse(u)
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q), ""))
    except:
        return u

def _clean_summary(s):
    if not s: return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()

def _domain_from_url(u):
    try: return urlparse(u).netloc.lower()
    except: return ""

def _is_allowed_feed(url):
    d = _domain_from_url(url)
    return any(d == dom or d.endswith("." + dom) for dom in ALLOWLIST_DOMAINS)

def parse_dt(s):
    try:
        return dtparse.parse(s)
    except:
        return None

def load_feeds(*files):
    feeds = []
    for path in files:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"): continue
                    parts = line.split()
                    if len(parts) == 1:
                        feeds.append(("", parts[0]))
                    else:
                        feeds.append((parts[0], parts[1]))
    return feeds

# ---------- Load feeds ----------
feeds = load_feeds("feeds.txt", "crypto_feeds.txt", "general_feeds.txt")

# ---------- Load archive ----------
os.makedirs(OUT_DIR, exist_ok=True)
old_items = []
if os.path.exists(JSONL_PATH):
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for ln in f:
            try: old_items.append(json.loads(ln))
            except: pass

# --- Migrate to date-only and add retrieved_date ---
def _migrate_legacy_item(o):
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

    if "retrieved_date" not in o or not o.get("retrieved_date"):
        iu = o.get("ingested_utc", "")
        o["retrieved_date"] = (iu[:10] if len(iu) >= 10 and iu[4] == "-" else
                               datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    o["summary"] = _clean_summary(o.get("summary", ""))
    return o

old_items = [_migrate_legacy_item(o) for o in old_items]

# ---------- Fetch new items ----------
new_items = []
seen_keys = {_normalize_url(i.get("url", "")) for i in old_items}

for src, feed_url in feeds:
    try:
        fp = feedparser.parse(feed_url)
    except:
        continue

    count = 0
    for e in fp.entries:
        if count >= PER_FEED_CAP:
            break

        link = _normalize_url(getattr(e, "link", ""))
        if link in seen_keys: continue

        title = getattr(e, "title", "").strip()
        summary = _clean_summary(getattr(e, "summary", ""))
        published_parsed = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        if published_parsed:
            dt_obj = datetime.fromtimestamp(time.mktime(published_parsed), tz=timezone.utc)
        else:
            dt_obj = parse_dt(getattr(e, "published", "")) or datetime.now(timezone.utc)

        if (datetime.now(timezone.utc) - dt_obj).days > SKIP_OLDER_DAYS:
            continue

        if not _is_allowed_feed(feed_url):
            if not _passes_keywords(title, summary):
                continue

        item = {
            "published_utc": dt_obj.strftime("%Y-%m-%d"),
            "retrieved_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "source": src or _domain_from_url(feed_url),
            "title": title,
            "url": link,
            "id_key": hashlib.sha256((title + link).encode("utf-8")).hexdigest(),
            "summary": summary,
            "ingested_utc": datetime.now(timezone.utc).isoformat()
        }

        seen_keys.add(link)
        new_items.append(item)
        count += 1

    time.sleep(SLEEP_BETWEEN_FEEDS)

# ---------- Merge and save ----------
all_items = old_items + new_items
all_items_sorted = sorted(
    all_items,
    key=lambda x: (x.get("published_utc", ""), x.get("ingested_utc", "")),
    reverse=True
)
all_items_sorted = all_items_sorted[:JSONL_MAX_ROWS]

with open(JSONL_PATH, "w", encoding="utf-8") as f:
    for o in all_items_sorted:
        f.write(json.dumps(o, ensure_ascii=False) + "\n")

latest = all_items_sorted[:LATEST_LIMIT]
with open(LATEST_PATH, "w", encoding="utf-8") as f:
    json.dump(latest, f, ensure_ascii=False, indent=2)

with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["published_utc", "retrieved_date", "source", "title", "url", "id_key", "summary", "ingested_utc"])
    for o in all_items_sorted:
        w.writerow([
            o.get("published_utc", ""),
            o.get("retrieved_date", ""),
            o.get("source", ""),
            o.get("title", ""),
            o.get("url", ""),
            o.get("id_key", ""),
            o.get("summary", ""),
            o.get("ingested_utc", "")
        ])
