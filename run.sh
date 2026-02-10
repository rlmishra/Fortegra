#!/usr/bin/env bash

set -e

if [ "$#" -ne 2 ]; then
  echo "Usage: ./run.sh <line_item_orders.csv> <packed_orders.csv>"
  exit 1
fi

SOURCE_A="$1"
SOURCE_B="$2"

echo "Running normalization..."
echo "Source A: $SOURCE_A"
echo "Source B: $SOURCE_B"
echo

python normalize_orders.py "$SOURCE_A" "$SOURCE_B"

echo
echo "Done."
echo "Generated canonical_events.jsonl"
