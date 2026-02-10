import json
import csv

input_file = "canonical_events.jsonl"
output_file = "canonical_events.xlsx.csv"

with open(input_file, "r", encoding="utf-8") as infile:
    rows = [json.loads(line) for line in infile if line.strip()]

# Use all keys as column headers
fieldnames = sorted({key for row in rows for key in row.keys()})

with open(output_file, "w", newline="", encoding="utf-8") as outfile:
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"CSV created: {output_file}")
