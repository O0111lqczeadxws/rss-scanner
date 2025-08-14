import argparse
import sys
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    ai_final_dir = Path("data/ai_final")
    drift_dir = Path("data/drift")

    errors = []

    if not any(ai_final_dir.glob(f"{args.date}*.jsonl")):
        errors.append(f"No AI final output found for {args.date}")

    if not any(drift_dir.glob(f"{args.date}_drift.json")):
        errors.append(f"No drift file found for {args.date}")

    if errors:
        for e in errors:
            print(f"[validate] {e}")
        sys.exit(1)

    print(f"[validate] Outputs look OK for {args.date}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
