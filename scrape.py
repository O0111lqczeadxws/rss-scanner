#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS scanner → JSONL/CSV (GitHub Pages friendly)

Networking hardening:
- Strict per-request timeout (default 12s) and bounded retries with backoff
- Always fetch bytes ourselves (requests/urllib) → feedparser.parse(bytes)
- HTML served? Auto-discover <link rel="alternate" type="rss|atom"> and retry
- Never hang on a single feed: worst case skip after retries

Other features preserved:
- Feed health tags in feed lists: [BROKEN]/[SKIP]/[CAP=20]
- new.jsonl (delta), latest.json, robust CSV (QUOTE_ALL)
- CLI: --force-refresh, --skip-days, plus new --timeout/--retries/--backoff
"""

import os, json, csv, re, html, hashlib, time, argparse, socket, gzip, io
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import urllib.request, urllib.error

import feedparser

# Optional deps (graceful if missing)
try:
    import requests
except Exception:
    requests = None

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None

# ==================== CONFIG ====================
OUT_DIR         = "docs"
ARCHIVE_DIR     = os.path.join(OUT_DIR, "archive")
ARCHIVE_SNAPSHOTS = True

JSONL_PATH   = os.path.join(OUT_DIR, "articles.jsonl")
CSV_PATH     = os.path.join(OUT_DIR, "articles.csv")
LATEST_PATH  = os.path.join(OUT_DIR, "latest.json")
STATUS_PATH  = os.path.join(OUT_DIR, "status.json")
NEW_PATH     = os.path.join(OUT_DIR, "new.jsonl")

FEED_FILES = [
    "feeds.txt",         # Tier 1 – official & macro movers
    "crypto_feeds.txt",  # Tier 2 – crypto high-signal
    "general_feeds.txt"  # Tier 3 – general candidates
]

# Limits / freshness
SKIP_OLDER_DAYS     = 10
PER_FEED_CAP        = 50
LATEST_LIMIT        = 1000
JSONL_MAX_ROWS      = 5000
SLEEP_BETWEEN_FEEDS = 0.6  # seconds

# HTTP defaults (overridable via CLI/env)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))
MAX_RETRIES     = int(os.getenv("MAX_RETRIES", "2"))
RETRY_BACKOFF   = float(os.getenv("RETRY_BACKOFF", "0.75"))

# Apply a global ceiling for any stray sockets
socket.setdefaulttimeout(REQUEST_TIMEOUT + 3)

UA = "Mozilla/5.0 (compatible; VANTA-RSS/1.0; +https://example.com/bot)"
REQ_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}

# ==================== VANTA2 TUNING ====================
KEYWORDS_INCLUDE = [
    # Core crypto assets & chains
    "bitcoin","btc","ethereum","eth","solana","dogecoin","xrp","cardano","bnb","tron",
    "polygon","matic","arbitrum","optimism","base","avalanche","near","aptos","sui",
    "chainlink","link","litecoin","fil","atom","dot",
    # Protocols / infra / tech
    "layer 2","l2","rollup","zk-rollup","zkevm","optimistic rollup","mev","restaking",
    "eigenlayer","staking","unstaking","slashing","validator","client release","hard fork",
    "soft fork","upgrade","eip","bip","cip","bridge","bridge exploit","reorg",
    # DeFi / tokenization / stablecoins
    "defi","amm","dex","cex","lending protocol","liquidation","stablecoin","tokenization",
    "rwa","real world asset","onchain treasury","mint","burn","redeem","circulating supply",
    # Markets / microstructure
    "etf","etn","creation unit","redemption","authorized participant","aum","basis trade",
    "futures","perpetuals","options","volatility","open interest","fund flow","net inflow",
    "short interest","liquidation cascade","order book","trading halt","circuit breaker",
    "listing","delisting","suspension","ipo","s-1","8-k","10-k","10-q","13f","prospectus",
    # Macro policy & data
    "fomc","minutes","dot plot","rate cut","rate hike","policy rate","terminal rate",
    "balance sheet","qe","qt","inflation","cpi","ppi","pce","employment","payrolls",
    "unemployment","gdp","retail sales","trade deficit","trade surplus","tariff","sanction",
    # Regulators / institutions / rules
    "sec","cftc","fincen","ofac","treasury","fdic","occ","doj","finra",
    "federal reserve","ecb","bank of england","boe","bank of japan","boj","snb","imf","bis",
    "fca","esma","eba","eiopa","iosco","mas","sfc","hkma","bafin","amf","sebi","rbi",
    "osfi","ciro","csa","statcan","bank of canada",
    "mica","mifid ii","basel iii","psd3","travel rule","aml","kyc","securities law",
    "consent order","cease and desist","settlement","wells notice","litigation release",
    # Energy / exchanges / wires
    "opec","iea","oil output","production quota","energy markets","diesel inventories","gasoline stocks",
    # Institutional & large actors
    "blackrock","ishares","fidelity","vanguard","ark invest","jpmorgan","goldman sachs",
    "morgan stanley","citadel","grayscale","microstrategy","cboe","cme","ice","dtcc","nasdaq","nyse"
]

KEYWORDS_EXCLUDE = [
    "casino","gambling","sportsbook","betting","lottery","xxx","porn",
    "giveaway","free airdrop","claim airdrop","win $","guaranteed returns","signal group",
    "pump and dump","shill","affiliate link","referral code","promo code",
    "sponsored post","paid content","partner content","brand studio",
    "top 10 coins","how to buy","best exchange","price prediction","price predictions","get rich",
    "technical analysis only","chart patterns only"
]

ALLOWLIST_DOMAINS = {
    # U.S. markets & policy
    "sec.gov","cftc.gov","federalreserve.gov","treasury.gov","home.treasury.gov",
    "ofac.treasury.gov","fincen.gov","fdic.gov","occ.gov","justice.gov",
    # CA / EU / intl.
    "bankofcanada.ca","statcan.gc.ca","www150.statcan.gc.ca","osfi-bsif.gc.ca","ciro.ca",
    "ecb.europa.eu","bankofengland.co.uk","bis.org","imf.org","eba.europa.eu","eiopa.europa.eu",
    "fca.org.uk","esma.europa.eu","bafin.de","amf-france.org",
    "boj.or.jp","fsa.go.jp","mas.gov.sg","sfc.hk","hkma.gov.hk","sebi.gov.in","rbi.org.in","snb.ch",
    # Energy / exchanges / wires
    "iea.org","opec.org","cmegroup.com","ice.com","cboe.com",
    "reuters.com","feeds.reuters.com","nasdaq.com","nyse.com","dtcc.com"
}

# ==================== HELPERS ====================
_rx_inc = re.compile("|".join([re.escape(k) for k in KEYWORDS_INCLUDE]), re.I) if KEYWORDS_INCLUDE else None
_rx_exc = re.compile("|".join([re.escape(k) for k in KEYWORDS_EXCLUDE]), re.I) if KEYWORDS_EXCLUDE else None

def _normalize_url(u: str) -> str:
    if not u:
        return ""
    try:
        p = urlparse(u)
        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True)
             if not k.lower().startswith("utm_")]
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q), ""))  # strip fragment
    except Exception:
        return u

def _domain(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

def _allowed(feed_url: str, link_url: str) -> bool:
    d1, d2 = _domain(feed_url), _domain(link_url)
    def on_list(d): return any(d == dom or d.endswith("." + dom) for dom in ALLOWLIST_DOMAINS)
    return on_list(d1) or on_list(d2)

def _clean_summary(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()

def _parse_dt(entry, feed_url: str):
    try:
        tt = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
        if tt:
            return datetime.fromtimestamp(time.mktime(tt), tz=timezone.utc)
    except Exception:
        pass
    for key in ("published", "updated", "created"):
        val = entry.get(key)
        if not val:
            continue
        try:
            tt = feedparser._parse_date(val)
            if tt:
                return datetime(*tt[:6], tzinfo=timezone.utc)
        except Exception:
            pass
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc)

def _passes_keywords(title: str, summary: str) -> bool:
    txt = f"{title or ''} {summary or ''}"
    if _rx_exc and _rx_exc.search(txt):
        return False
    if _rx_inc and not _rx_inc.search(txt):
        return False
    return True

def _dedupe_key(title: str, link: str) -> str:
    t = (title or "").strip().lower()
    t = re.sub(r"\s+", " ", t)
    return hashlib.sha256(f"{t}|{_normalize_url(link)}".encode("utf-8")).hexdigest()

# ---- Feed list parsing with health tags ----
_TAG_RE = re.compile(r"\[(.*?)\]")
def _parse_feed_line(line: str):
    tags = {}
    for m in _TAG_RE.findall(line):
        if "=" in m:
            k, v = m.split("=", 1)
            tags[k.strip().upper()] = v.strip()
        else:
            tags[m.strip().upper()] = True
    line_clean = _TAG_RE.sub("", line).strip()
    src, url = "", ""
    if "\t" in line_clean:
        src, url = line_clean.split("\t", 1)
    else:
        parts = line_clean.split()
        if len(parts) >= 2 and parts[1].startswith("http"):
            src, url = parts[0], " ".join(parts[1:])
        else:
            url = line_clean
    return (src.strip(), url.strip(), tags)

def _load_feeds():
    out = []
    for ff in FEED_FILES:
        if not os.path.exists(ff):
            continue
        with open(ff, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                src, url, tags = _parse_feed_line(line)
                out.append((src, url, tags))
    return out

# ---- CSV sanitization ----
def _csv_clean(x) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.replace("\r", " ").replace("\n", " ").replace("\u2028", " ").replace("\u2029", " ")
    s = s.replace("\x00", " ")
    return s

# ---- Low-level fetch (timeout + retries + gzip) ----
def _urllib_fetch(url: str, timeout: int) -> tuple[bytes, str]:
    req = urllib.request.Request(url, headers=REQ_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
        enc = (r.headers.get("Content-Encoding") or "").lower()
        if enc == "gzip":
            try:
                data = gzip.decompress(data)
            except Exception:
                pass
        ctype = (r.headers.get("Content-Type") or "").lower()
        return data, ctype

def _fetch_bytes(url: str, timeout: int, retries: int, backoff: float) -> tuple[bytes, str]:
    last_err = None
    for attempt in range(retries + 1):
        try:
            if requests:
                r = requests.get(url, headers=REQ_HEADERS, timeout=timeout)
                content = r.content  # requests auto-decompresses by default
                ctype = (r.headers.get("Content-Type") or "").lower()
                # allow 2xx only
                if 200 <= r.status_code < 300 and content:
                    return content, ctype
                last_err = RuntimeError(f"HTTP {r.status_code}")
            else:
                content, ctype = _urllib_fetch(url, timeout)
                if content:
                    return content, ctype
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, socket.timeout) as e:
            last_err = e
        except Exception as e:
            last_err = e
        time.sleep(backoff * (attempt + 1))
    raise last_err if last_err else TimeoutError("timeout")

_XML_PROLOG_RE = re.compile(r'<\?xml[^>]*encoding=["\'].*?["\'][^>]*\?>', re.I)
def _fix_xml_encoding(s: bytes) -> str:
    try:
        text = s.decode("utf-8", errors="replace")
    except Exception:
        text = s.decode("latin-1", errors="replace")
    if _XML_PROLOG_RE.search(text):
        text = _XML_PROLOG_RE.sub('<?xml version="1.0" encoding="utf-8"?>', text, count=1)
    text = text.replace("\x00", " ")
    return text

def _discover_rss_in_html(html_text: str, base_url: str) -> str:
    if not BeautifulSoup:
        return ""
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        for link in soup.find_all("link"):
            rel = [r.lower() for r in (link.get("rel") or [])]
            typ = (link.get("type") or "").lower()
            href = link.get("href") or ""
            if ("alternate" in rel) and ("rss" in typ or "atom" in typ or "xml" in typ):
                from urllib.parse import urljoin
                return urljoin(base_url, href)
    except Exception:
        pass
    return ""

# ---- Robust parse entry point (bounded time) ----
def _parse_with_fallback(url: str, errors_list: list, timeout: int, retries: int, backoff: float):
    """
    1) Fetch bytes with strict timeout/retries
    2) If HTML, discover alternate RSS and re-fetch
    3) Parse bytes via feedparser.parse(bytes)
    """
    try:
        raw, ctype = _fetch_bytes(url, timeout, retries, backoff)
    except Exception as ex:
        errors_list.append({"source": url, "error": f"fetch error: {ex}"})
        return None

    # If HTML, try to discover an alternate feed
    if "text/html" in ctype or (raw[:64].lstrip().startswith(b"<!DOCTYPE html") or raw[:32].lstrip().lower().startswith(b"<html")):
        alt = ""
        if BeautifulSoup:
            try:
                html_text = raw.decode("utf-8", errors="replace")
            except Exception:
                html_text = raw.decode("latin-1", errors="replace")
            alt = _discover_rss_in_html(html_text, url)
        if alt:
            try:
                raw, ctype = _fetch_bytes(alt, timeout, retries, backoff)
            except Exception as ex:
                errors_list.append({"source": url, "error": f"alt feed fetch error: {ex}"})
                # fall through and try to parse original HTML bytes (will likely be bozo)

    fixed = _fix_xml_encoding(raw)
    try:
        parsed = feedparser.parse(fixed)
        return parsed
    except Exception as ex:
        errors_list.append({"source": url, "error": f"parse bytes error: {ex}"})
        return None

# ==================== MAIN ====================
def main():
    parser = argparse.ArgumentParser(description="RSS → JSONL/CSV scraper")
    parser.add_argument("--force-refresh", action="store_true",
                        help="Ignore age filter (skip-days=0) and rebuild outputs fresh for this run.")
    parser.add_argument("--skip-days", type=int, default=None,
                        help="Override SKIP_OLDER_DAYS just for this run.")
    parser.add_argument("--timeout", type=int, default=None, help="Per-request timeout seconds (default from env/12).")
    parser.add_argument("--retries", type=int, default=None, help="Max retries per request (default from env/2).")
    parser.add_argument("--backoff", type=float, default=None, help="Retry backoff seconds multiplier (env/0.75).")
    args = parser.parse_args()

    # Apply CLI overrides
    global REQUEST_TIMEOUT, MAX_RETRIES, RETRY_BACKOFF
    if args.timeout is not None: REQUEST_TIMEOUT = max(3, int(args.timeout))
    if args.retries is not None: MAX_RETRIES = max(0, int(args.retries))
    if args.backoff is not None: RETRY_BACKOFF = max(0.0, float(args.backoff))
    socket.setdefaulttimeout(REQUEST_TIMEOUT + 3)

    skip_days = 0 if args.force_refresh else (args.skip_days if args.skip_days is not None else SKIP_OLDER_DAYS)
    cutoff = datetime.now(timezone.utc) - timedelta(days=skip_days)

    os.makedirs(OUT_DIR, exist_ok=True)
    if ARCHIVE_SNAPSHOTS:
        os.makedirs(ARCHIVE_DIR, exist_ok=True)

    start_ts = datetime.now(timezone.utc)
    feeds = _load_feeds()

    # Load previous JSONL and migrate fields
    old_items = []
    if os.path.exists(JSONL_PATH) and not args.force_refresh:
        with open(JSONL_PATH, "r", encoding="utf-8") as f:
            for ln in f:
                try:
                    o = json.loads(ln)
                    # migrate published_utc to YYYY-MM-DD and add retrieved_date
                    pu = o.get("published_utc", "")
                    if pu:
                        if len(pu) >= 10 and pu[4] == "-" and pu[7] == "-":
                            o["published_utc"] = pu[:10]
                        else:
                            try:
                                dd = datetime.fromisoformat(pu.replace("Z", "+00:00"))
                            except Exception:
                                dd = datetime.now(timezone.utc)
                            o["published_utc"] = dd.strftime("%Y-%m-%d")
                    else:
                        iu = o.get("ingested_utc", "")
                        o["published_utc"] = (iu[:10] if len(iu) >= 10 and iu[4] == "-"
                                              else datetime.now(timezone.utc).strftime("%Y-%m-%d"))
                    if not o.get("retrieved_date"):
                        iu = o.get("ingested_utc", "")
                        o["retrieved_date"] = (iu[:10] if len(iu) >= 10 and iu[4] == "-"
                                               else datetime.now(timezone.utc).strftime("%Y-%m-%d"))
                    o["summary"] = _clean_summary(o.get("summary", ""))
                    old_items.append(o)
                except Exception:
                    pass

    exist_ids = {o.get("id_key") for o in old_items if o.get("id_key")}
    new_items = []
    seen_title_url = set()
    by_source = {}
    stats = {
        "feeds_total": len(feeds),
        "feeds_error": 0,
        "feeds_timeout": 0,
        "feeds_http_error": 0,
        "entries_seen": 0,
        "too_old": 0,
        "dup_title_url": 0,
        "dup_id": 0,
        "passed_keywords": 0,
        "passed_allowlist": 0,
        "failed_all_filters": 0
    }
    errors = []

    for (src_name, feed_url, tags) in feeds:
        tag_keys = {k.upper(): v for k, v in (tags or {}).items()}
        if "BROKEN" in tag_keys or "SKIP" in tag_keys:
            by_source[src_name or feed_url] = by_source.get(src_name or feed_url, 0) + 0
            print(f"[FEED] {src_name or feed_url} → SKIPPED (tagged)")
            continue

        per_cap = PER_FEED_CAP
        if "CAP" in tag_keys:
            try:
                per_cap = max(1, int(tag_keys["CAP"]))
            except Exception:
                pass

        added, skipped = 0, 0
        t0 = time.time()
        try:
            parsed = _parse_with_fallback(feed_url, errors_list=errors,
                                          timeout=REQUEST_TIMEOUT, retries=MAX_RETRIES, backoff=RETRY_BACKOFF)
            if parsed is None:
                stats["feeds_error"] += 1
                errors.append({"source": src_name or feed_url, "error": "fatal parse failure"})
                by_source[src_name or feed_url] = by_source.get(src_name or feed_url, 0) + 0
                elapsed = time.time() - t0
                print(f"[FEED] {src_name or feed_url} → Added: 0, Skipped: 0 (fatal) | {elapsed:.2f}s")
                time.sleep(SLEEP_BETWEEN_FEEDS)
                continue

            entries = list(getattr(parsed, "entries", []) or [])[:per_cap]

            if int(getattr(parsed, "bozo", 0) or 0):
                errors.append({"source": src_name or feed_url,
                               "error": str(getattr(parsed, "bozo_exception", ""))})

            for e in entries:
                stats["entries_seen"] += 1
                try:
                    title = (e.get("title") or "").strip()
                    link  = _normalize_url((e.get("link") or "").strip())
                    summary = _clean_summary(e.get("summary") or e.get("description") or "")
                    pub_dt = _parse_dt(e, feed_url)

                    if pub_dt < cutoff:
                        stats["too_old"] += 1
                        skipped += 1
                        continue

                    allowed = _allowed(feed_url, link)
                    if _rx_exc and _rx_exc.search(f"{title} {summary}"):
                        stats["failed_all_filters"] += 1
                        skipped += 1
                        continue
                    if not allowed:
                        if not _passes_keywords(title, summary):
                            stats["failed_all_filters"] += 1
                            skipped += 1
                            continue
                        else:
                            stats["passed_keywords"] += 1
                    else:
                        stats["passed_allowlist"] += 1

                    dk = _dedupe_key(title, link)
                    if dk in seen_title_url:
                        stats["dup_title_url"] += 1
                        skipped += 1
                        continue
                    seen_title_url.add(dk)

                    ingested_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    src_label = (src_name or getattr(parsed.feed, "title", "") or "").strip()
                    base = f"{src_label}|{title}|{link}|{pub_dt.strftime('%Y-%m-%d')}"
                    id_key = hashlib.sha256(base.encode("utf-8")).hexdigest()
                    if id_key in exist_ids:
                        stats["dup_id"] += 1
                        skipped += 1
                        continue
                    exist_ids.add(id_key)

                    item = {
                        "published_utc": pub_dt.strftime("%Y-%m-%d"),
                        "retrieved_date": ingested_now[:10],
                        "source": src_label,
                        "title": title,
                        "url": link,
                        "id_key": id_key,
                        "summary": summary,
                        "ingested_utc": ingested_now
                    }
                    new_items.append(item)
                    added += 1
                except Exception as inner_ex:
                    errors.append({"source": src_name or feed_url, "error": f"entry error: {inner_ex}"})
                    skipped += 1
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, socket.timeout) as net_ex:
            stats["feeds_error"] += 1
            if isinstance(net_ex, (TimeoutError, socket.timeout)):
                stats["feeds_timeout"] += 1
            else:
                stats["feeds_http_error"] += 1
            errors.append({"source": src_name or feed_url, "error": f"net: {net_ex.__class__.__name__}: {net_ex}"})
        except Exception as ex:
            stats["feeds_error"] += 1
            errors.append({"source": src_name or feed_url, "error": f"outer error: {ex}"})

        elapsed = time.time() - t0
        by_source[src_name or feed_url] = by_source.get(src_name or feed_url, 0) + added
        print(f"[FEED] {src_name or feed_url} → Added: {added}, Skipped: {skipped} | {elapsed:.2f}s")
        time.sleep(SLEEP_BETWEEN_FEEDS)

    # Merge, sort, cap
    all_items = ([] if args.force_refresh else old_items) + new_items
    all_items_sorted = sorted(
        all_items,
        key=lambda x: (x.get("published_utc",""), x.get("ingested_utc","")),
        reverse=True
    )
    keep = all_items_sorted[:JSONL_MAX_ROWS]
    latest = keep[:LATEST_LIMIT]

    # ---------- Write outputs (docs/*) ----------
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for obj in keep:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    with open(LATEST_PATH, "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)

    with open(NEW_PATH, "w", encoding="utf-8") as f:
        for obj in new_items:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n")
        w.writerow(["published_utc","retrieved_date","source","title","url","id_key"])
        for obj in keep:
            w.writerow([
                _csv_clean(obj.get("published_utc","")),
                _csv_clean(obj.get("retrieved_date","")),
                _csv_clean(obj.get("source","")),
                _csv_clean(obj.get("title","")),
                _csv_clean(obj.get("url","")),
                _csv_clean(obj.get("id_key",""))
            ])

    # ---------- Append to data/processed/YYYY-MM-DD.jsonl (new items only) ----------
    processed_appended = 0
    proc_dir = os.path.join("data", "processed")
    os.makedirs(proc_dir, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    proc_path = os.path.join(proc_dir, f"{date_str}.jsonl")

    existing_ids_processed = set()
    if os.path.exists(proc_path):
        with open(proc_path, "r", encoding="utf-8") as f:
            for ln in f:
                try:
                    obj = json.loads(ln)
                    k = obj.get("id_key")
                    if k:
                        existing_ids_processed.add(k)
                except Exception:
                    pass

    delta_items = [o for o in new_items if o.get("id_key") not in existing_ids_processed]
    mode = "a" if os.path.exists(proc_path) else "w"
    if delta_items:
        with open(proc_path, mode, encoding="utf-8") as f:
            for obj in delta_items:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        processed_appended = len(delta_items)
    print(f"[processed] {processed_appended} new rows appended to {proc_path} (mode={mode})")

    # Optional dated snapshot of the CSV
    snapshot_err = None
    if ARCHIVE_SNAPSHOTS:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        snap_path = os.path.join(ARCHIVE_DIR, f"articles-{ts}.csv")
        try:
            with open(CSV_PATH, "r", encoding="utf-8") as src, \
                 open(snap_path, "w", newline="", encoding="utf-8") as dst:
                for ln in src:
                    dst.write(ln)
        except Exception as ex:
            snapshot_err = str(ex)

    # Status
    end_ts = datetime.now(timezone.utc)
    status = {
        "started_utc": start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ended_utc": end_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "new_items_this_run": len(new_items),
        "total_in_latest": len(latest),
        "by_source": dict(by_source),
        "errors": (
            ([{"source": "snapshot", "error": f"csv snapshot failed: {snapshot_err}"}] if snapshot_err else [])
            + errors
        ),
        "feed_files": [ff for ff in FEED_FILES if os.path.exists(ff)],
        "filters": {
            "include": KEYWORDS_INCLUDE[:10] + (["..."] if len(KEYWORDS_INCLUDE) > 10 else []),
            "exclude": KEYWORDS_EXCLUDE[:10] + (["..."] if len(KEYWORDS_EXCLUDE) > 10 else []),
            "passed_keywords": stats["passed_keywords"],
            "passed_allowlist": stats["passed_allowlist"],
            "failed_all_filters": stats["failed_all_filters"]
        },
        "stats": {
            "feeds_total": stats["feeds_total"],
            "feeds_error": stats["feeds_error"],
            "feeds_timeout": stats["feeds_timeout"],
            "feeds_http_error": stats["feeds_http_error"],
            "entries_seen": stats["entries_seen"],
            "too_old": stats["too_old"],
            "dup_title_url": stats["dup_title_url"],
            "dup_id": stats["dup_id"]
        },
        "limits": {
            "skip_older_days": skip_days,
            "per_feed_cap": PER_FEED_CAP,
            "jsonl_max_rows": JSONL_MAX_ROWS,
            "latest_limit": LATEST_LIMIT,
            "timeout_s": REQUEST_TIMEOUT,
            "retries": MAX_RETRIES,
            "backoff": RETRY_BACKOFF
        },
        "processed": {
            "file": proc_path,
            "appended_new": processed_appended
        }
    }
    with open(STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

    print(json.dumps(status, indent=2))

if __name__ == "__main__":
    main()
