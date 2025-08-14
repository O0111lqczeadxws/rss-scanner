# build_ai_bundle.py
# Purpose: Create AI-ready "basket" files (AI bundles) from processed news JSONL.
# Input (default):  data/processed/YYYY-MM-DD*.jsonl
# Output (default): data/ai_bundles/YYYY-MM-DD.jsonl
#
# Run from repo root (Windows PowerShell):
#   py -m src.build_ai_bundle --date 2025-08-14
#
# Notes:
# - Robust to missing/invalid config; falls back to sane defaults.
# - Skips records missing published/retrieved timestamps.
# - Uses simple salient sentence scoring and VADER sentiment for rule_features.

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import re
from collections import Counter
from typing import Any, Dict, List, Tuple

from dateutil.parser import isoparse
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import yaml

AN = SentimentIntensityAnalyzer()


# ------------------------------- helpers ------------------------------------ #

def sent_split(text: str) -> List[str]:
    """Naive sentence splitter."""
    if not text:
        return []
    parts = re.split(r'(?<=[.!?])\s+', text.strip())
    return [s.strip() for s in parts if s.strip()]


def tokenize(text: str) -> List[str]:
    """Simple alnum-ish tokenizer that keeps $, #, +, -, . for tickers/terms."""
    return re.findall(r"[A-Za-z0-9$#\-\+\.]+", (text or ""))


def top_k_salient(text: str, k: int = 3, boost: List[str] | None = None) -> List[str]:
    """
    Pick top-k salient sentences by TF-style scoring with gentle keyword boost.
    Falls back to [] if no sentences.
    """
    sents = sent_split(text)
    if not sents:
        return []

    full_lower = " ".join(sents).lower()
    tf = Counter(tokenize(full_lower))
    boost_set = set([b.lower() for b in (boost or [])])

    scored: List[Tuple[int, str]] = []
    for s in sents:
        toks = tokenize(s.lower())
        score = sum(tf.get(t, 0) for t in toks) + sum(2 for t in toks if t in boost_set)
        scored.append((score, s))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [s for _, s in scored[:k]]


def minutes_between(a_iso: str, b_iso: str) -> int:
    """Return non-negative minutes between two ISO timestamps."""
    try:
        a = isoparse(a_iso)
        b = isoparse(b_iso)
        diff = (b - a).total_seconds() / 60.0
        return int(diff) if diff >= 0 else 0
    except Exception:
        return 0


def stable_id(url: str | None, title: str) -> str:
    """Stable 16-char id from url+title."""
    h = hashlib.sha1()
    h.update((url or "").encode("utf-8"))
    h.update(("||" + (title or "")).encode("utf-8"))
    return h.hexdigest()[:16]


def load_rules(path: str = "config/vanta_rules.yaml") -> Dict[str, Any]:
    """
    Load YAML rules; return defaults if file missing/invalid.
    """
    default = {
        "sources": {},
        "lexicons": {
            "institutional": ["SEC", "ETF", "BlackRock", "Fidelity", "custody", "19b-4", "S-1", "regulator"],
            "retail": ["retail", "trader", "FOMO", "Reddit", "TikTok", "meme"],
            "vol_triggers": ["halt", "liquidation", "circuit", "expiry", "expiration"],
        },
    }
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict) or "lexicons" not in data:
            return default
        return data
    except FileNotFoundError:
        return default
    except Exception:
        return default


def count_terms(text: str, terms: List[str]) -> int:
    """
    Count occurrences of each term in text (case-insensitive) using word-boundary matching.
    """
    T = (text or "").lower()
    total = 0
    for t in terms or []:
        pat = rf"\b{re.escape(t.lower())}\b"
        total += len(re.findall(pat, T))
    return total


# ------------------------------ core builder -------------------------------- #

def build(in_glob: str, out_path: str, rules: Dict[str, Any]) -> Tuple[int, int]:
    """
    Read processed JSONL files (in_glob), write AI bundle JSONL (out_path).
    Returns (read_count, wrote_count).
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    read = 0
    wrote = 0

    with open(out_path, "w", encoding="utf-8") as out:
        for fp in sorted(glob.glob(in_glob)):
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        read += 1
                        try:
                            it = json.loads(line)
                            if not isinstance(it, dict):
                                continue
                        except Exception:
                            continue

                        title = (it.get("title") or "").strip()
                        url = it.get("url")
                        body = it.get("body") or it.get("summary") or title
                        summary = (it.get("summary") or title).strip()

                        # keywords may be str or list; normalize to list
                        kw = it.get("keywords")
                        if isinstance(kw, str):
                            keywords = [kw]
                        else:
                            keywords = kw or []

                        pub = it.get("published_utc") or it.get("published")
                        ret = it.get("retrieved_utc") or it.get("retrieved")
                        if not (pub and ret):
                            # skip items without proper timestamps
                            continue

                        salient = top_k_salient(body, k=3, boost=keywords) or [summary]

                        # rule-derived features
                        text_for_rules = " ".join([title, summary] + salient)
                        tone = AN.polarity_scores(text_for_rules)["compound"]
                        inst = count_terms(text_for_rules, rules["lexicons"].get("institutional", []))
                        retail = count_terms(text_for_rules, rules["lexicons"].get("retail", []))

                        vflags: List[str] = []
                        Tlow = text_for_rules.lower()
                        for v in rules["lexicons"].get("vol_triggers", []):
                            if re.search(rf"\b{re.escape(v.lower())}\b", Tlow):
                                vflags.append(v)
                        # de-dup but keep deterministic order
                        seen = set()
                        vol_flags = [x for x in vflags if not (x in seen or seen.add(x))]

                        freshness_min = minutes_between(pub, ret)
                        source = it.get("source")
                        source_weight = float(rules.get("sources", {}).get(source, 1.0))

                        bundle = {
                            "id": it.get("id") or stable_id(url, title),
                            "source": source,
                            "url": url,
                            "title": title,
                            "published_utc": pub,
                            "retrieved_utc": ret,
                            "keywords": keywords,
                            "summary": summary,
                            "salient_sentences": salient,
                            "rule_features": {
                                "tone_vader": round(tone, 4),
                                "agency_counts": {"institutional": int(inst), "retail": int(retail)},
                                "vol_flags": vol_flags,
                                "freshness_min": int(freshness_min),
                                "source_weight": source_weight,
                            },
                        }

                        out.write(json.dumps(bundle, ensure_ascii=False) + "\n")
                        wrote += 1

            except FileNotFoundError:
                # skip missing file in glob
                continue

    print(f"[build_ai_bundle] read={read} wrote={wrote} -> {out_path}")
    return read, wrote


# --------------------------------- entry ------------------------------------ #

def main() -> None:
    ap = argparse.ArgumentParser(description="Build AI bundle (basket) JSONL files for VANTA AI layer.")
    ap.add_argument("--date", help="YYYY-MM-DD (UTC). Default: today (UTC).", default=None)
    ap.add_argument("--in_glob", help="Glob for processed inputs. Default: data/processed/{date}*.jsonl", default=None)
    ap.add_argument("--out", help="Output path. Default: data/ai_bundles/{date}.jsonl", default=None)
    args = ap.parse_args()

    from datetime import datetime, timezone
    date = args.date or datetime.now(timezone.utc).date().isoformat()

    in_glob = args.in_glob or f"data/processed/{date}*.jsonl"
    out_path = args.out or f"data/ai_bundles/{date}.jsonl"

    rules = load_rules("config/vanta_rules.yaml")
    build(in_glob, out_path, rules)


if __name__ == "__main__":
    main()
