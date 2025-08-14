# ai_layer/interpreter.py
# Merge ai_bundles + ai_out and compute a simple VANTA-style score + daily report.
# Run: py -m ai_layer.interpreter --date 2025-08-14

from __future__ import annotations
import argparse, json, os, math, time
from collections import Counter

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if line:
                try: yield json.loads(line)
                except: continue

def save_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def freshness_w(minutes:int) -> float:
    # 2h half-life style weight
    return 1.0 / (1.0 + (minutes or 0)/120.0)

def compute_score(bundle:dict, ai:dict) -> float:
    rf = bundle.get("rule_features", {})
    fresh_min = int(rf.get("freshness_min", 0))
    vol = 1 if rf.get("vol_flags") else 0
    agency_inst = float(ai.get("agency", {}).get("institutional", 0.5))
    certainty = float(ai.get("certainty", ai.get("confidence", 0.5)))
    sent = abs(float(ai.get("sentiment", 0.0)))

    # Components → 0..100
    score = (
        30.0*certainty +                   # model certainty
        25.0*sent +                        # sentiment strength
        15.0*freshness_w(fresh_min) +      # recency
        15.0*(1.0 if vol else 0.0) +       # vol trigger present
        15.0*agency_inst                   # institutional tilt = higher weight
    )
    return round(min(100.0, score), 1)

def merge(date:str, in_bundles:str|None=None, in_ai:str|None=None, out_path:str|None=None, report_path:str|None=None):
    in_bundles = in_bundles or f"data/ai_bundles/{date}.jsonl"
    in_ai      = in_ai or f"data/ai_out/{date}.jsonl"
    out_path   = out_path or f"data/ai_final/{date}.jsonl"
    report_path= report_path or f"reports/daily/{date}-ai.md"

    # index AI by id
    ai_by_id = {r["id"]: r for r in load_jsonl(in_ai)}
    rows, stats = [], {
        "n": 0, "avg": 0.0, "stances": Counter(), "tags": Counter(), "vol": 0
    }

    for b in load_jsonl(in_bundles):
        aid = b.get("id")
        ai = ai_by_id.get(aid)
        if not ai:
            # keep record but mark missing
            merged = {
                "id": aid,
                "source": b.get("source"),
                "title": b.get("title"),
                "published_utc": b.get("published_utc"),
                "retrieved_utc": b.get("retrieved_utc"),
                "summary": b.get("summary"),
                "rule_features": b.get("rule_features"),
                "ai_missing": True,
                "score": None
            }
            rows.append(merged)
            continue

        score = compute_score(b, ai)
        merged = {
            "id": aid,
            "source": b.get("source"),
            "url": b.get("url"),
            "title": b.get("title"),
            "published_utc": b.get("published_utc"),
            "retrieved_utc": b.get("retrieved_utc"),
            "summary": b.get("summary"),
            "salient_sentences": b.get("salient_sentences"),
            "keywords": b.get("keywords"),
            "rule_features": b.get("rule_features"),
            "ai": ai,
            "score": score,
            # diagnostics
            "ai_rule_delta_sent": round(float(ai.get("sentiment",0.0)) - float(b.get("rule_features",{}).get("tone_vader",0.0)), 3)
        }
        rows.append(merged)

        # stats
        stats["n"] += 1
        stats["avg"] += score
        stats["stances"][ai.get("stance","neutral")] += 1
        stats["vol"] += 1 if b.get("rule_features",{}).get("vol_flags") else 0
        for t in ai.get("narrative_tags", []):
            stats["tags"][t] += 1

    # finalize average
    if stats["n"] > 0:
        stats["avg"] = round(stats["avg"]/stats["n"], 1)

    save_jsonl(out_path, rows)
    write_report(report_path, date, stats, rows)
    print(f"[interpreter] merged={stats['n']} avg_score={stats['avg']} -> {out_path}")
    return rows, stats

def write_report(path:str, date:str, stats:dict, rows:list[dict]):
    lines = []
    lines.append(f"# AI Narrative Report — {date}")
    lines.append(f"- Items with AI: **{stats['n']}**   Avg score: **{stats['avg']}**   Vol triggers: **{stats['vol']}**")
    if stats["stances"]:
        total = stats["n"] or 1
        stance_line = " / ".join(f"{k}: {int(100*v/total)}%" for k,v in stats["stances"].items())
        lines.append(f"- Stance split: {stance_line}")
    if stats["tags"]:
        top_tags = ", ".join(f"{t}×{c}" for t,c in stats["tags"].most_common(6))
        lines.append(f"- Top tags: {top_tags}")
    lines.append("\n## Top items")
    top = sorted([r for r in rows if r.get("score") is not None], key=lambda x: x["score"], reverse=True)[:10]
    for r in top:
        src = r.get("source") or "?"
        ttl = r.get("title") or "?"
        sc  = r.get("score")
        st  = (r.get("ai") or {}).get("stance")
        lines.append(f"- **{sc}** · {src} · *{st}* — {ttl}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--in-bundles", dest="in_bundles", default=None)
    ap.add_argument("--in-ai", dest="in_ai", default=None)
    ap.add_argument("--out", dest="out_path", default=None)
    ap.add_argument("--report", dest="report_path", default=None)
    a = ap.parse_args()
    merge(a.date, a.in_bundles, a.in_ai, a.out_path, a.report_path)

if __name__ == "__main__":
    main()
