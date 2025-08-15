# subtask_trump_putin.py
from pathlib import Path
import json
from datetime import datetime

# Keywords to match (case-insensitive)
KEYWORDS = [
    "trump putin alaska",
    "trump meeting putin",
    "putin meeting trump",
    "alaska summit trump putin",
    "august 15 2025 trump putin",
    "trump putin anchorage"
]

# Paths - adjust if needed
AI_FINAL_DIR = Path("data/ai_final")
SUBTASK_OUT = Path("data/subtasks/trump_putin_meeting.jsonl")

def keyword_match(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in KEYWORDS)

def run_subtask():
    SUBTASK_OUT.parent.mkdir(parents=True, exist_ok=True)

    # Find the latest ai_final file(s)
    for file in sorted(AI_FINAL_DIR.glob("*.jsonl"), reverse=True):
        with file.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue

                title = item.get("title", "")
                summary = item.get("summary", "")
                if keyword_match(title) or keyword_match(summary):
                    out_data = {
                        "published_utc": item.get("published_utc"),
                        "source": item.get("source"),
                        "title": title,
                        "summary": summary,
                        "url": item.get("url")
                    }
                    with SUBTASK_OUT.open("a", encoding="utf-8") as outf:
                        outf.write(json.dumps(out_data) + "\n")

if __name__ == "__main__":
    run_subtask()
