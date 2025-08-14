import argparse
import sys
from pathlib import Path
from datetime import datetime

def detect_latest_date(processed_dir: Path) -> str:
    files = sorted(processed_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    return files[0].stem[:10]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD (UTC). Leave empty to auto-detect.")
    args = parser.parse_args()

    processed_dir = Path("data/processed")
    ai_final_dir = Path("data/ai_final")
    drift_dir = Path("data/drift")

    # Detect latest date if not provided
    date = args.date
    if not date:
        date = detect_latest_date(processed_dir)
        if not date:
            print("::error::No date provided and no processed files found.")
            input("Press Enter to exit...")
            sys.exit(1)
        print(f"[validate] No date provided, using latest: {date}")

    errors = []

    if not any(ai_final_dir.glob(f"{date}*.jsonl")):
        errors.append(f"No AI final output found for {date}")

    if not any(drift_dir.glob(f"{date}_drift.json")):
        errors.append(f"No drift file found for {date}")

    if errors:
        for e in errors:
            print(f"::error::{e}")  # GitHub Actions error annotation
        sys.exit(1)

    print(f"::notice::Validation passed for {date}")  # GitHub Actions notice annotation
    return 0

if __name__ == "__main__":
    sys.exit(main())
