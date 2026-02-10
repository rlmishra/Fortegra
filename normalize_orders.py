import csv
import json
import sys
from datetime import datetime

def utc_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def emit(event, out_file):
    out_file.write(json.dumps(event) + "\n")

# ---------- System A Adapter (Line-per-item) ----------
def process_system_a(path, out_file):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event = {
                "event_type": "OrderItemCreated",
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "transaction_id": row["txn_id"],
                "item_sku": row["item_sku"],
                "quantity": int(row["qty"]),
                "unit_price": float(row["unit_price"]),
                "event_date": row["event_date"],
                "source": row["source"],
                "file_id": row["file_id"],
                "row_id": int(row["row_id"]),
                "ingested_at": utc_now()
            }
            emit(event, out_file)

# ---------- System B Adapter (Packed-SKU) ----------
def process_system_b(path, out_file):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for i in range(1, 4):
                sku = row.get(f"sku_{i}")
                price = row.get(f"price_{i}")

                if sku and sku.strip():
                    event = {
                        "event_type": "OrderItemCreated",
                        "order_id": row["order_id"],
                        "customer_id": row["customer_id"],
                        "order_ref": row["order_ref"],
                        "item_sku": sku,
                        "quantity": 1,
                        "unit_price": float(price),
                        "event_date": row["order_date"],
                        "source": row["source"],
                        "file_id": row["file_id"],
                        "row_id": int(row["row_id"]),
                        "ingested_at": utc_now()
                    }
                    emit(event, out_file)

def main():
    if len(sys.argv) != 3:
        print("Usage: python normalize_orders.py <Source_A.csv> <Source_B.csv>")
        sys.exit(1)

    source_a = sys.argv[1]
    source_b = sys.argv[2]

    out_path = "canonical_events.jsonl"
    with open(out_path, "w", encoding="utf-8") as out_file:
        process_system_a(source_a, out_file)
        process_system_b(source_b, out_file)

    print(f"Done. Output written to: {out_path}")

if __name__ == "__main__":
    main()
