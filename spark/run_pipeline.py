import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from ingest import load_all
from clean import clean_all
from enrich import enrich_all

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
RAW_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')


def run():
    print("=== SPARK PIPELINE STARTING ===")
    raw = load_all(RAW_DIR)
    cleaned = clean_all(raw)
    enriched = enrich_all(cleaned)

    tables_to_export = [
        "orders", "order_items", "order_payments",
        "order_reviews", "products", "customers",
        "sellers", "order_summary"
    ]

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for name in tables_to_export:
        path = os.path.join(OUTPUT_DIR, name)
        enriched[name].write.mode("overwrite").parquet(path)
        print(f"Exported {name} → {path}")

    print("=== PIPELINE COMPLETE ===")


if __name__ == "__main__":
    run()
