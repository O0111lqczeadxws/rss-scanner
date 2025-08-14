# ai_layer/infer.py
from __future__ import annotations
import argparse, json, os, re, time

def clamp(x, lo, hi): 
    return lo if x < lo else hi if x > hi else x

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if line:
                try:
                    yield json.loads(line)
                except Exception:
                    continue

def save_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# Heuristic lexicons (mock layer)
VOL_NEG = {"halt","liquidation","circuit","bankrupt","insolvency"}
BEAR_WORDS = {"reject","delay","outflow","fraud","crackdown","ban","probe","lawsuit","charges","downgrade","breach"}
BULL_WORDS = {"approval","approve","inflow","surge","breakout","ath","rate cut","rate cuts","cuts rates","greenlight"}

def keyword_score(text: str) -> tuple[float,float,int,int]:
    """Return (bull_kw_score, bear_kw_score, bull_hits, bear_hits)"""
    t = text.lower()
    b_hits = sum(1 for w in BULL_WORDS if w in t)
    br_hits = sum(1 for w in BEAR_WORDS if w in t)
    # each hit worth 0.25, soft cap at 0.75
    return min(0.25*b_hits, 0.75), min(0.25*br_hits, 0.75), b_hits, br_hits

def infer_one(bundle: dict) -> dict:
    rf = bundle.get("rule_features", {})
    tone = float(rf.get("tone_vader", 0.0))
    inst = int(rf.get("agency_counts", {}).get("institutional", 0))
    retail = int(rf.get("agency_counts", {}).get("retail", 0))
    total_ag = inst + retail

    fresh_min = int(rf.get("freshness_min", 0))
    vol_flags = set(rf.get("vol_flags", []))

    text = " ".join([
        bundle.get("title",""), 
        bundle.get("summary",""),
        " ".join(bundle.get("salient_sentences", []))
    ])

    # 1) Scores: tone contributes smoothly; keywords add nudges
    bull_s, bear_s = 0.0, 0.0
    if tone > 0.05: bull_s += min(tone, 1.0) * 0.8
    if tone < -0.05: bear_s += min(abs(tone), 1.0) * 0.8

    bull_kw, bear_kw, bull_hits, bear_hits = keyword_score(text)
    bull_s += bull_kw
    bear_s += bear_kw

    # Volatility flags slightly lean bearish if negative types present
    if any(v.lower() in VOL_NEG for v in vol_flags):
        bear_s += 0.15

    # 2) Decide stance with margin; small margins => neutral
    margin = bull_s - bear_s
    if margin > 0.15:
        stance = "bullish"
    elif margin < -0.15:
        stance = "bearish"
    else:
        stance = "neutral"

    # 3) Certainty: strength + freshness; penalize mixed signals
    mixed = (tone > 0.1 and bear_kw > bull_kw) or (tone < -0.1 and bull_kw > bear_kw)
    base_cert = 0.35 + 0.4*min(abs(tone),1.0) + 0.25*min(abs(margin),1.0)
    if vol_flags: base_cert += 0.05
    if fresh_min <= 60: base_cert += 0.05
    if mixed: base_cert -= 0.2
    certainty = clamp(base_cert, 0.2, 0.98)

    # 4) Agency split; handle zero-count case explicitly
    if total_ag == 0:
        agency = {"institutional": 0.5, "retail": 0.5}
        agency_note = "Agency-Unknown"
    else:
        inst_ratio = inst / total_ag
        agency = {"institutional": round(inst_ratio, 2), "retail": round(1 - inst_ratio, 2)}
        agency_note = None

    # 5) Narrative tags
    tags = []
    tl = text.lower()
    if any(k in tl for k in ["sec","regulator","19b-4","s-1"]): tags.append("Regulatory-Gatekeeping")
    if any(k in tl for k in ["etf","fidelity","blackrock","inflow","outflow"]): tags.append("Institutional-Flow")
    if any(k in tl for k in ["cpi","inflation","rate cut","rate cuts","fed","fomc"]): tags.append("Macro-Policy")
    if vol_flags: tags.append("Volatility-Trigger")
    if mixed: tags.append("Mixed-Signals")
    if agency_note: tags.append(agency_note)

    # 6) VANTA1/2 fields
    v1 = {"stance": stance, "tone": round(tone,3), "tags": tags}
    v2 = {
        "power_axis": "institutional" if agency["institutional"] >= 0.5 else "retail",
        "agency_inst": agency["institutional"],
        "freshness_min": fresh_min,
        "lag_class": "fresh" if fresh_min <= 60 else "normal" if fresh_min <= 240 else "stale"
    }

    # 7) Rationales (clear + concrete)
    r = []
    r.append(f"Tone={tone:+.2f}, KW bull={bull_hits} bear={bear_hits}, margin={margin:+.2f} → {stance}.")
    if vol_flags:
        r.append(f"Vol flags: {', '.join(sorted(v for v in vol_flags))}.")
    if total_ag == 0:
        r.append("No agency keywords → default 50/50.")
    else:
        r.append(f"Agency inst={inst} vs retail={retail} (split {agency['institutional']:.2f}/{agency['retail']:.2f}).")
    if mixed:
        r.append("Mixed signals (tone vs keywords) → certainty reduced.")

    return {
        "id": bundle.get("id"),
        "model": "mock-ai",
        "version": "v1.1",
        "sentiment": round(tone, 3),
        "stance": stance,
        "certainty": round(certainty, 2),
        "agency": agency,
        "narrative_tags": tags,
        "vanta1": v1,
        "vanta2": v2,
        "rationales": r,
        "confidence": round(certainty, 2),
        "latency_ms": 0
    }

def run(date: str, in_path: str | None = None, out_path: str | None = None):
    in_path = in_path or f"data/ai_bundles/{date}.jsonl"
    out_path = out_path or f"data/ai_out/{date}.jsonl"
    t0 = time.time()
    outs = [infer_one(b) for b in load_jsonl(in_path)]
    save_jsonl(out_path, outs)
    print(f"[ai_infer] wrote={len(outs)} -> {out_path}  in {int((time.time()-t0)*1000)}ms")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--in", dest="in_path", default=None)
    ap.add_argument("--out", dest="out_path", default=None)
    a = ap.parse_args()
    run(a.date, a.in_path, a.out_path)

if __name__ == "__main__":
    main()
