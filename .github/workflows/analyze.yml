#!/usr/bin/env python3
"""
Analyze feeds (VANTA AI) - single entrypoint.
Runs:
  1. build_ai_bundle
  2. ai_layer.infer
  3. ai_layer.interpreter
  4. Drift scoring (integrated here)
  5. ai_layer.interpreter (with drift)
  6. src.validate_outputs

Logs everything to reports/daily/DATE.log.
"""

import argparse
import sys
import traceback
import statistics
import json
from pathlib import Path
from datetime import datetime, timedelta
import subprocess

# Directories
DATA_PROCESSED_DIR = Path("data/processed")
AI_FINAL_DIR = Path("data/ai_final")
DRIFT_DIR = Path("data/drift")
REPORTS_DAILY_DIR = Path("reports/daily")

STAGES = [
    ("Build AI bundles", ["python", "-m", "src.build_ai_bundle"]),
    ("AI infer (VANTA1/2)", ["python", "-m", "ai_layer.infer"]),
    ("Interpret & score (final)", ["python", "-m", "ai_layer.interpreter"]),
    # Drift integrated below instead of calling ai_layer.drfit
    ("Rebuild report (with drift)", ["python", "-m", "ai_layer.interpreter"]),
    ("Validate outputs", ["python", "-m", "src.validate_outputs"]),
]

def run_stage(name, cmd, date):
    print(f"\n=== Stage: {name} ===")
    full_cmd = cmd + ["--date", date]
    try:
        subprocess.run(full_cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Stage '{name}' failed with exit code {e.returncode}")
        sys.exit(e.returncode)
    except Exception as e:
        print(f"[ERROR] Stage '{name}' crashed: {e}")
        traceback.print_exc()
        sys.exit(1)

def resolve_date(input_date: str) -> str:
    """Resolve target date: explicit > today UTC > latest processed file."""
    if input_date:
        return input_date
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if list(DATA_PROCESSED_DIR.glob(f"{today}*.jsonl")):
        return today
    latest = sorted(DATA_PROCESSED_DIR.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    if latest:
        alt = latest[0].stem[:10]
        try:
            datetime.strptime(alt, "%Y-%m-%d")
            return alt
        except ValueError:
            pass
    return today

def count_processed(date: str) -> int:
    files = list(DATA_PROCESSED_DIR.glob(f"{date}*.jsonl"))
    count = 0
    for f in files:
        with f.open("r", encoding="utf-8") as fp:
            count += sum(1 for _ in fp)
    return count

def ensure_dirs():
    for d in (DRIFT_DIR, REPORTS_DAILY_DIR):
        d.mkdir(parents=True, exist_ok=True)

# --- Drift logic integrated here ---
def load_jsonl(path: Path):
    items = []
    if not path.exists():
        return items
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return items

def safe_mean(vals):
    vals = [v for v in vals if isinstance(v, (int, float))]
    return round(statistics.mean(vals), 4) if vals else None

def safe_range(vals):
    vals = [v for v in vals if isinstance(v, (int, float))]
    return (min(vals), max(vals)) if vals else None

def extract_sentiment(item):
    if "sentiment" in item and isinstance(item["sentiment"], (int, float)):
        return item["sentiment"]
    if "ai" in item and isinstance(item["ai"], dict):
        val = item["ai"].get("sentiment")
        return val if isinstance(val, (int, float)) else None
    return None

def extract_score(item):
    if "score" in item and isinstance(item["score"], (int, float)):
        return item["score"]
    if "ai" in item and isinstance(item["ai"], dict):
        val = item["ai"].get("score")
        return val if isinstance(val, (int, float)) else None
    return None

def run_drift(date: str, lookback: int = 7):
    print(f"\n=== Stage: Drift scoring (lookback {lookback} days) ===")
    history = []
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    for i in range(lookback):
        day = date_obj - timedelta(days=i)
        for fp in AI_FINAL_DIR.glob(f"{day.date()}*.jsonl"):
            history.extend(load_jsonl(fp))

    drift_file = DRIFT_DIR / f"{date}_drift.json"

    if not history:
        print(f"[drift] No AI final data found for last {lookback} days")
        drift_file.write_text(json.dumps({
            "date": date,
            "lookback_days": lookback,
            "items_count": 0,
            "avg_sentiment": None,
            "avg_score": None,
            "sentiment_range": None,
            "score_range": None
        }, indent=2))
        return

    sentiments = [extract_sentiment(x) for x in history]
    scores = [extract_score(x) for x in history]

    drift_stats = {
        "date": date,
        "lookback_days": lookback,
        "items_count": len(history),
        "avg_sentiment": safe_mean(sentiments),
        "avg_score": safe_mean(scores),
        "sentiment_range": safe_range(sentiments),
        "score_range": safe_range(scores)
    }

    drift_file.write_text(json.dumps(drift_stats, indent=2))
    print(f"[drift] Saved drift stats to {drift_file}")

# --- Main pipeline ---
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="", help="YYYY-MM-DD (UTC). Leave empty for auto.")
    parser.add_argument("--lookback", type=int, default=7, help="Days to look back for drift scoring")
    args = parser.parse_args()

    date = resolve_date(args.date)
    print(f"[analyze] Using DATE={date}")

    count = count_processed(date)
    if count == 0:
        print(f"[analyze] No processed items for {date} — exiting early.")
        sys.exit(0)

    ensure_dirs()

    log_file = REPORTS_DAILY_DIR / f"{date}.log"
    with log_file.open("w", encoding="utf-8") as lf:
        sys.stdout = sys.stderr = lf
        try:
            # Pre-drift stages
            for name, cmd in STAGES[:3]:
                run_stage(name, cmd, date)

            # Drift stage
            run_drift(date, lookback=args.lookback)

            # Post-drift stages
            for name, cmd in STAGES[3:]:
                run_stage(name, cmd, date)

        except SystemExit as e:
            raise
        except Exception as e:
            print(f"[FATAL] Unexpected crash: {e}")
            traceback.print_exc()
            sys.exit(1)

    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    print(f"[analyze] Analysis complete for {date}. Log saved to {log_file}")

if __name__ == "__main__":
    main()
