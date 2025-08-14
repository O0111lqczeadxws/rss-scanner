#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS scanner → JSONL/CSV (GitHub Pages friendly)

Enhancements vs. base version:
- Feed health tags in feed lists:
  [BROKEN] or [SKIP] -> skip feed
  [CAP=20]           -> per-feed item cap override
- Robust parsing pipeline:
  1) feedparser.parse(url)
  2) If bozo/encoding/XML issues: requests.get + XML prolog fix + re-parse
  3) If HTML served: try BeautifulSoup to discover <link rel="alternate" ... rss> and parse that
- new.jsonl written each run with only NEW items (delta)
- CSV hardening: QUOTE_ALL + sanitized fields + '\n' line endings
- CLI flags:
  --force-refresh   -> treat as full refresh (skip-days=0), write clean new/latest
  --skip-days N     -> override freshness for this run

Requires: feedparser
Optional: requests, bs4
"""

import os, json, csv, re, html, hashlib, time, argparse
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

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
ARCHIVE_DIR     = os.path.join(OUT_DIR, "archive")     # for optional dated snapshots
ARCHIVE_SNAPSHOTS = True                                # set False to disable

JSONL_PATH   = os.path.join(OUT_DIR, "articles.jsonl")
CSV_PATH     = os.path.join(OUT_DIR, "articles.csv")
LATEST_PATH  = os.path.join(OUT_DIR, "latest.json")
STATUS_PATH  = os.path.join(OUT_DIR, "status.json")
NEW_PATH     = os.path.join(OUT_DIR, "new.jsonl")       # delta: only new items this run

FEED_FILES = [
    "feeds.txt",         # Tier 1 – official & macro movers
    "crypto_feeds.txt",  # Tier 2 – crypto high-signal
    "general_feeds.txt"  # Tier 3 – general candidates
]

# Limits / freshness (can be overridden via --skip-days)
SKIP_OLDER_DAYS     = 10
PER_FEED_CAP        = 50
LATEST_LIMIT        = 1000
JSONL_MAX_ROWS      = 5000
SLEEP_BETWEEN_FEEDS = 0.6  # seconds

# HTTP defaults for requests fallback
REQ_TIMEOUT = 12
REQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (RSS-Scanner; +https://example.com/bot) Python",
    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
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
    # Bypass include keywords if either the feed domain OR the link domain is on the allowlist
    d1, d2 = _domain(feed_url), _domain(link_url)
    def on_list(d): return any(d == dom or d.endswith("." + dom) for dom in ALLOWLIST_DOMAINS)
    return on_list(d1) or on_list(d2)

def _clean_summary(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)  # strip HTML tags
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()

def _parse_dt(entry, feed_url: str):
    # Prefer structured time from feedparser, fallback to common strings, else now
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
_TAG_RE = re.compile(r"\[(.*?)\]")   # matches [ ... ]
def _parse_feed_line(line: str):
    """
    Returns: (source_name, url, tags_dict)
    Supports tags like [BROKEN], [SKIP], [CAP=20] anywhere in the line.
    """
    tags = {}
    for m in _TAG_RE.findall(line):
        if "=" in m:
            k, v = m.split("=", 1)
            tags[k.strip().upper()] = v.strip()
        else:
            tags[m.strip().upper()] = True

    # Remove tag blocks from line
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
    """Sanitize for CSV: remove hard line breaks and exotic separators that can
    confuse strict CSV previews; normalize to plain spaces."""
    if x is None:
        return ""
    s = str(x)
    s = s.replace("\r", " ").replace("\n", " ").replace("\u2028", " ").replace("\u2029", " ")
    s = s.replace("\x00", " ")
    return s

# ---- requests/bs4 fallback helpers ----
_XML_PROLOG_RE = re.compile(r'<\?xml[^>]*encoding=["\'].*?["\'][^>]*\?>', re.I)
def _fix_xml_encoding(s: bytes) -> str:
    """
    Attempt to normalize XML encoding to UTF-8 and strip obvious bad control chars.
    Returns text (utf-8 decoded).
    """
    try:
        text = s.decode("utf-8", errors="replace")
    except Exception:
        # try latin-1 as last resort
        text = s.decode("latin-1", errors="replace")
    # Normalize XML prolog encoding to utf-8 (helps "us-ascii declared" issues)
    if _XML_PROLOG_RE.search(text):
        text = _XML_PROLOG_RE.sub('<?xml version="1.0" encoding="utf-8"?>', text, count=1)
    # strip NULLs
    text = text.replace("\x00", " ")
    return text

def _discover_rss_in_html(html_text: str, base_url: str) -> str:
    """If a page is HTML, try to find an alternate RSS/Atom link."""
    if not BeautifulSoup:
        return ""
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        for link in soup.find_all("link"):
            rel = (link.get("rel") or [])
            typ = (link.get("type") or "").lower()
            href = link.get("href") or ""
            if ("alternate" in [r.lower() for r in rel]) and ("rss" in typ or "atom" in typ):
                # Resolve relative href
                try:
                    from urllib.parse import urljoin
                    return urljoin(base_url, href)
                except Exception:
                    return href
    except Exception:
        pass
    return ""

def _parse_with_fallback(url: str, errors_list: list):
    """
    Try feedparser; on failure use requests to re-fetch and re-parse.
    If HTML is served, try to discover the real feed link via BeautifulSoup.
    Returns a feedparser-like result object (with .entries), or None on fatal error.
    """
    try:
        parsed = feedparser.parse(url)
    except Exception as ex:
        errors_list.append({"source": url, "error": f"feedparser explode: {ex}"})
        parsed = None

    bozo = int(getattr(parsed, "bozo", 0) or 0) if parsed else 1
    if parsed and not bozo and getattr(parsed, "entries", None):
        return parsed  # good

    # Fallback only if requests is available
    if not requests:
        if parsed:
            # Return even if bozo; we'll handle entry loop robustly
            return parsed
        errors_list.append({"source": url, "error": "requests not available for fallback"})
        return None

    # Try to fetch raw
    try:
        r = requests.get(url, headers=REQ_HEADERS, timeout=REQ_TIMEOUT)
        ct = (r.headers.get("Content-Type") or "").lower()
        content = r.content
    except Exception as ex:
        errors_list.append({"source": url, "error": f"requests error: {ex}"})
        return parsed if parsed else None

    # If HTML was served, attempt to discover <link rel="alternate" type="application/rss+xml">
    if "text/html" in ct or (ct == "" and content.strip().startswith(b"<!DOCTYPE html")):
        if BeautifulSoup:
            try:
                html_text = content.decode(r.apparent_encoding or "utf-8", errors="replace")
            except Exception:
                html_text = content.decode("utf-8", errors="replace")
            alt = _discover_rss_in_html(html_text, url)
            if alt:
                try:
                    r2 = requests.get(alt, headers=REQ_HEADERS, timeout=REQ_TIMEOUT)
                    fixed = _fix_xml_encoding(r2.content)
                    parsed2 = feedparser.parse(fixed)
                    bozo2 = int(getattr(parsed2, "bozo", 0) or 0)
                    if not bozo2 and getattr(parsed2, "entries", None):
                        return parsed2
                except Exception as ex2:
                    errors_list.append({"source": url, "error": f"alt rss fetch failed: {ex2}"})
        # Fall through to attempt to parse HTML text anyway (will likely fail)
        fixed = _fix_xml_encoding(content)
        parsed_html = feedparser.parse(fixed)
        return parsed_html

    # Not HTML → likely XML but with bad prolog/encoding; normalize and re-parse
    fixed = _fix_xml_encoding(content)
    parsed2 = feedparser.parse(fixed)
    return parsed2

# ==================== MAIN ====================
def main():
    parser = argparse.ArgumentParser(description="RSS → JSONL/CSV scraper")
    parser.add_argument("--force-refresh", action="store_true",
                        help="Ignore age filter (skip-days=0) and rebuild outputs fresh for this run.")
    parser.add_argument("--skip-days", type=int, default=None,
                        help="Override SKIP_OLDER_DAYS just for this run.")
    args = parser.parse_args()

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
                    # swallow bad lines
                    pass

    exist_ids = {o.get("id_key") for o in old_items if o.get("id_key")}
    new_items = []
    seen_title_url = set()
    by_source = {}
    stats = {
        "feeds_total": len(feeds),
        "feeds_error": 0,
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
        # Respect health tags
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
        try:
            parsed = _parse_with_fallback(feed_url, errors_list=errors)
            if parsed is None:
                stats["feeds_error"] += 1
                errors.append({"source": src_name or feed_url, "error": "fatal parse failure"})
                by_source[src_name or feed_url] = by_source.get(src_name or feed_url, 0) + 0
                print(f"[FEED] {src_name or feed_url} → Added: 0, Skipped: 0 (fatal)")
                time.sleep(SLEEP_BETWEEN_FEEDS)
                continue

            # If feedparser bozo but still has entries, proceed carefully
            entries = list(getattr(parsed, "entries", []) or [])[:per_cap]

            # Count bozo as error but keep going
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

                    # Freshness
                    if pub_dt < cutoff:
                        stats["too_old"] += 1
                        skipped += 1
                        continue

                    # Filter logic (allowlist bypasses INCLUDE but still must pass EXCLUDE)
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

                    # Dedupe across this run
                    dk = _dedupe_key(title, link)
                    if dk in seen_title_url:
                        stats["dup_title_url"] += 1
                        skipped += 1
                        continue
                    seen_title_url.add(dk)

                    # Build item
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
        except Exception as ex:
            stats["feeds_error"] += 1
            errors.append({"source": src_name or feed_url, "error": f"outer error: {ex}"})

        by_source[src_name or feed_url] = by_source.get(src_name or feed_url, 0) + added
        print(f"[FEED] {src_name or feed_url} → Added: {added}, Skipped: {skipped}")
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

    # ---------- Write outputs ----------
    # JSONL (full)
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for obj in keep:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # latest.json
    with open(LATEST_PATH, "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)

    # new.jsonl (delta only)
    with open(NEW_PATH, "w", encoding="utf-8") as f:
        for obj in new_items:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # CSV (strict and sanitized)
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
            **{k: v for k, v in stats.items() if k not in ("passed_keywords","passed_allowlist","failed_all_filters")}
        }
    }
    with open(STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

    # Also print for CI logs
    print(json.dumps(status, indent=2))

if __name__ == "__main__":
    main()
