import feedparser
import json
import os
from datetime import datetime, timezone
from html import unescape
from urllib.parse import urlparse

# ======== CONFIG ========
FEED_FILES = [
    "feeds.txt",           # Tier 1 – Official & macro movers
    "crypto_feeds.txt",    # Tier 2 – Crypto high-signal
    "general_feeds.txt"    # Tier 3 – General candidates
]

JSONL_PATH = "articles.jsonl"
CSV_PATH = "articles.csv"
JSONL_MAX_ROWS = 5000
MAX_ITEMS_PER_FEED = 50

# ---------- VANTA2 keyword & domain tuning ----------
KEYWORDS_INCLUDE = [
    "bitcoin","btc","ethereum","eth","solana","dogecoin","xrp","cardano","bnb","tron",
    "polygon","matic","arbitrum","optimism","base","avalanche","near","aptos","sui",
    "chainlink","link","litecoin","fil","atom","dot",
    "layer 2","l2","rollup","zk-rollup","zkEVM","optimistic rollup","mev","restaking",
    "eigenlayer","staking","unstaking","slashing","validator","client release","hard fork",
    "soft fork","upgrade","eip","bip","cip","bridge","bridge exploit","reorg",
    "defi","amm","dex","cex","lending protocol","liquidation","stablecoin","tokenization",
    "rwa","real world asset","onchain treasury","mint","burn","redeem","circulating supply",
    "etf","etn","creation unit","redemption","authorized participant","aum","basis trade",
    "futures","perpetuals","options","volatility","open interest","fund flow","net inflow",
    "short interest","liquidation cascade","order book","trading halt","circuit breaker",
    "listing","delisting","suspension","ipo","s-1","8-k","10-k","10-q","13f","prospectus",
    "fomc","minutes","dot plot","rate cut","rate hike","policy rate","terminal rate",
    "balance sheet","qe","qt","inflation","cpi","ppi","pce","employment","payrolls",
    "unemployment","gdp","retail sales","trade deficit","trade surplus","tariff","sanction",
    "sec","cftc","fincen","ofac","treasury","fdic","occ","doj","finra",
    "federal reserve","ecb","bank of england","boe","bank of japan","boj","snb","imf","bis",
    "fca","esma","eba","eiopa","iosco","mas","sfc","hkma","bafin","amf","sebi","rbi",
    "osfi","ciro","csa","statcan","bank of canada",
    "mica","mifid ii","basel iii","psd3","travel rule","aml","kyc","securities law",
    "consent order","cease and desist","settlement","wells notice","litigation release",
    "opec","iea","oil output","production quota","energy markets","diesel inventories","gasoline stocks",
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
    "sec.gov","cftc.gov","federalreserve.gov","treasury.gov","home.treasury.gov",
    "ofac.treasury.gov","fincen.gov","fdic.gov","occ.gov","justice.gov",
    "bankofcanada.ca","statcan.gc.ca","www150.statcan.gc.ca","osfi-bsif.gc.ca","ciro.ca",
    "ecb.europa.eu","bankofengland.co.uk","bis.org","imf.org","eba.europa.eu","eiopa.europa.eu",
    "fca.org.uk","esma.europa.eu","bafin.de","amf-france.org",
    "boj.or.jp","fsa.go.jp","mas.gov.sg","sfc.hk","hkma.gov.hk","sebi.gov.in","rbi.org.in","snb.ch",
    "iea.org","opec.org","cmegroup.com","ice.com","cboe.com",
    "reuters.com","feeds.reuters.com","nasdaq.com","nyse.com","dtcc.com"
}

# ======== HELPERS ========
def parse_dt(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime(*feedparser._parse_date(s)[:6], tzinfo=timezone.utc)
        except Exception:
            return None

def _clean_summary(s):
    return unescape(s or "").replace("\n", " ").strip()

def match_keywords(text):
    t = text.lower()
    if any(k in t for k in KEYWORDS_EXCLUDE):
        return False
    if any(k in t for k in KEYWORDS_INCLUDE):
        return True
    return False

def domain_allowed(url):
    host = urlparse(url).netloc.lower()
    return any(host.endswith(d) for d in ALLOWLIST_DOMAINS)

# ======== LOAD OLD ========
old_items = []
if os.path.exists(JSONL_PATH):
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for ln in f:
            try:
                old_items.append(json.loads(ln))
            except Exception:
                pass

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

# ======== LOAD FEEDS ========
feed_urls = []
for ff in FEED_FILES:
    if os.path.exists(ff):
        with open(ff, "r", encoding="utf-8") as f:
            feed_urls.extend([ln.strip() for ln in f if ln.strip() and not ln.startswith("#")])

new_items = []
for url in feed_urls:
    try:
        parsed = feedparser.parse(url)
        for entry in parsed.entries[:MAX_ITEMS_PER_FEED]:
            title = entry.get("title", "")
            summary = entry.get("summary", "")
            link = entry.get("link", "")
            if not (match_keywords(title + " " + summary) or domain_allowed(link)):
                continue
            pu = entry.get("published", "") or entry.get("updated", "")
            dt = parse_dt(pu) or datetime.now(timezone.utc)
            published_date = dt.strftime("%Y-%m-%d")
            ingested_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            new_items.append({
                "published_utc": published_date,
                "retrieved_date": ingested_now[:10],
                "source": parsed.feed.get("title", ""),
                "title": title.strip(),
                "url": link,
                "id_key": entry.get("id", link),
                "summary": _clean_summary(summary),
                "ingested_utc": ingested_now
            })
    except Exception as e:
        print(f"Error fetching {url}: {e}")

# ======== MERGE & SAVE ========
all_items = old_items + new_items
all_items_sorted = sorted(all_items, key=lambda x: (x.get("published_utc", ""), x.get("ingested_utc", "")), reverse=True)
all_items_sorted = all_items_sorted[:JSONL_MAX_ROWS]

with open(JSONL_PATH, "w", encoding="utf-8") as f:
    for item in all_items_sorted:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

with open(CSV_PATH, "w", encoding="utf-8") as f:
    f.write("published_utc,retrieved_date,source,title,url,id_key\n")
    for item in all_items_sorted:
        f.write(f"{item['published_utc']},{item['retrieved_date']},{item['source']},{item['title']},{item['url']},{item['id_key']}\n")
