#!/usr/bin/env bash
set -euo pipefail

CSV_PATH="${1:-data/in/reviews.csv}"
OUT_DIR="${2:-data/out}"
BATCH_SIZE="${3:-25}"
SLEEP_S="${4:-0}"

JSONL="$OUT_DIR/project_summaries.jsonl"

mkdir -p "$OUT_DIR"

echo "CSV: $CSV_PATH"
echo "OUT: $OUT_DIR"
echo "BATCH_SIZE: $BATCH_SIZE"
echo "SLEEP_S: $SLEEP_S"
echo

while true; do
  BEFORE=0
  AFTER=0

  if [[ -f "$JSONL" ]]; then
    BEFORE=$(wc -l < "$JSONL" | tr -d ' ')
  fi

  python scripts/generate_project_summaries.py \
    --csv "$CSV_PATH" \
    --out "$OUT_DIR" \
    --batch-size "$BATCH_SIZE" \
    --resume \
    --sleep-s "$SLEEP_S"

  if [[ -f "$JSONL" ]]; then
    AFTER=$(wc -l < "$JSONL" | tr -d ' ')
  fi

  echo "Progress: $BEFORE -> $AFTER summaries"

  if [[ "$AFTER" -le "$BEFORE" ]]; then
    echo "No new summaries written. Assuming done."
    break
  fi
done
