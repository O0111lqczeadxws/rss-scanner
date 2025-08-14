#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS scanner → JSONL/CSV (GitHub Pages friendly)

- Writes canonical outputs under ./docs
- CSV uses QUOTE_ALL, lineterminator="\n", and content sanitization to avoid
  GitHub "Illegal quoting" preview errors
- Optional dated CSV snapshots under ./docs/archive/

Requires: feedparser
"""

import os, json, csv, re, html, hashlib, time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import feedparser

# ==================== CONFIG ====================
OUT_DIR       = "docs"
ARCHIVE_DIR   = os.path.join(OUT_DIR, "archive")     # for optional dated snapshots
ARCHIVE_SNAPSHOTS = True                              # set False to disable

JSONL_PATH   = os.path.join(OUT_DIR, "articles.jsonl")
CSV_PATH     = os.path.join(OUT_DIR, "articles.csv")
LATEST_PATH  = os.path.join(OUT_DIR, "latest.json")
STATUS_PATH  = os.path.join(OUT_DIR, "status.json")

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
            # feedparser can parse many formats via _parse_date
            tt = feedparser._parse_date(val)
            if tt:
                return datetime(*tt[:6], tzinfo=timezone.utc)
        except Exception:
            pass
        try:
            # ISO-ish fallback
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
                src, url = "", ""
                if "\t" in line:
                    src, url = line.split("\t", 1)
                else:
                    parts = line.split()
                    if len(parts) >= 2 and parts[1].startswith("http"):
                        src, url = parts[0], " ".join(parts[1:])
                    else:
                        url = line
                out.append((src.strip(), url.strip()))
    return out

def _csv_clean(x) -> str:
    """Sanitize for CSV: remove hard line breaks and exotic separators that can
    confuse strict CSV previews; normalize to plain spaces."""
    if x is None:
        return ""
    s = str(x)
    # remove CR/LF and Unicode line/paragraph separators
    s = s.replace("\r", " ").replace("\n", " ").replace("\u2028", " ").replace("\u2029", " ")
    # strip NULLs if any
    s = s.replace("\x00", " ")
    return s

# ==================== MAIN ====================
def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if ARCHIVE_SNAPSHOTS:
        os.makedirs(ARCHIVE_DIR, exist_ok=True)

    start_ts = datetime.now(timezone.utc)
    feeds = _load_feeds()

    # Load previous JSONL and migrate fields
    old_items = []
    if os.path.exists(JSONL_PATH):
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
    cutoff = datetime.now(timezone.utc) - timedelta(days=SKIP_OLDER_DAYS)

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

    for (src_name, feed_url) in feeds:
        added, skipped = 0, 0
        try:
            parsed = feedparser.parse(feed_url)
            if getattr(parsed, "bozo", 0):
                errors.append({"source": src_name or feed_url,
                               "error": str(getattr(parsed, "bozo_exception", ""))})
            for e in parsed.entries[:PER_FEED_CAP]:
                stats["entries_seen"] += 1

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
                base = f"{(src_name or parsed.feed.get('title','')).strip()}|{title}|{link}|{pub_dt.strftime('%Y-%m-%d')}"
                id_key = hashlib.sha256(base.encode("utf-8")).hexdigest()
                if id_key in exist_ids:
                    stats["dup_id"] += 1
                    skipped += 1
                    continue
                exist_ids.add(id_key)

                item = {
                    "published_utc": pub_dt.strftime("%Y-%m-%d"),
                    "retrieved_date": ingested_now[:10],
                    "source": (src_name or parsed.feed.get("title","")).strip(),
                    "title": title,
                    "url": link,
                    "id_key": id_key,
                    "summary": summary,
                    "ingested_utc": ingested_now
                }
                new_items.append(item)
                added += 1
        except Exception as ex:
            stats["feeds_error"] += 1
            errors.append({"source": src_name or feed_url, "error": str(ex)})
        by_source[src_name or feed_url] = by_source.get(src_name or feed_url, 0) + added
        print(f"[FEED] {src_name or feed_url} → Added: {added}, Skipped: {skipped}")
        time.sleep(SLEEP_BETWEEN_FEEDS)

    # Merge, sort, cap
    all_items = old_items + new_items
    all_items_sorted = sorted(
        all_items,
        key=lambda x: (x.get("published_utc",""), x.get("ingested_utc","")),
        reverse=True
    )
    keep = all_items_sorted[:JSONL_MAX_ROWS]
    latest = keep[:LATEST_LIMIT]

    # ---------- Write outputs ----------
    # JSONL
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for obj in keep:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # latest.json
    with open(LATEST_PATH, "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)

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
    if ARCHIVE_SNAPSHOTS:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        snap_path = os.path.join(ARCHIVE_DIR, f"articles-{ts}.csv")
        # write snapshot atomically by copying content we just wrote
        try:
            with open(CSV_PATH, "r", encoding="utf-8") as src, \
                 open(snap_path, "w", newline="", encoding="utf-8") as dst:
                for ln in src:
                    dst.write(ln)
        except Exception as ex:
            errors.append({"source": "snapshot", "error": f"csv snapshot failed: {ex}"})

    # Status
    end_ts = datetime.now(timezone.utc)
    status = {
        "started_utc": start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ended_utc": end_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "new_items_this_run": len(new_items),
        "total_in_latest": len(latest),
        "by_source": dict(by_source),
        "errors": errors,
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
