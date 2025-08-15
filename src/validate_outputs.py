import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone

def detect_latest_date(processed_dir: Path) -> str:
    """Find the latest processed file's date (YYYY-MM-DD) by mtime."""
    files = sorted(processed_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    return files[0].stem[:10]

def get_current_utc_date() -> str:
    """Return current UTC date as YYYY-MM-DD."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD (UTC). Leave empty to auto-detect.")
    args = parser.parse_args()

    processed_dir = Path("data/processed")
    ai_final_dir = Path("data/ai_final")
    drift_dir = Path("data/drift")

    # Step 1: Use provided date, else try latest file in processed_dir
    date = args.date
    if not date:
        date = detect_latest_date(processed_dir)
        if date:
            print(f"[validate] No date provided, using latest file date: {date}")
        else:
            # Step 2: Fallback to current UTC date if no files found
            date = get_current_utc_date()
            print(f"[validate] No processed files found, using current UTC date: {date}")

    errors = []

    if not any(ai_final_dir.glob(f"{date}*.jsonl")):
        errors.append(f"No AI final output found for {date}")

    if not any(drift_dir.glob(f"{date}_drift.json")):
        errors.append(f"No drift file found for {date}")

    if errors:
        for e in errors:
            print(f"::error::{e}")
        sys.exit(1)

    print(f"::notice::Validation passed for {date}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
