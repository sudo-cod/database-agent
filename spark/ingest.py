import os
from pyspark.sql import SparkSession


def get_spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("OlistIngestion") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


# Map canonical table names to possible filenames
_FILE_CANDIDATES = {
    "customers": ["olist_customers_dataset.csv", "customers_dataset.csv"],
    "orders": ["olist_orders_dataset.csv", "orders_dataset.csv"],
    "order_items": ["olist_order_items_dataset.csv", "order_items_dataset.csv"],
    "order_payments": ["olist_order_payments_dataset.csv", "order_payments_dataset.csv"],
    "order_reviews": ["olist_order_reviews_dataset.csv", "order_reviews_dataset.csv"],
    "products": ["olist_products_dataset.csv", "products_dataset.csv"],
    "sellers": ["olist_sellers_dataset.csv", "sellers_dataset.csv"],
    "category_translation": ["product_category_name_translation.csv"],
    "geolocation": ["olist_geolocation_dataset.csv", "geolocation_dataset.csv"],
}


def _find_file(raw_dir, candidates):
    for name in candidates:
        path = os.path.join(raw_dir, name)
        if os.path.exists(path):
            return path
    return None
'''
spark.read: Accesses the DataFrameReader, which is the entry point for loading data into Spark.
.option("header", "true"): Tells Spark that the first row of CSV contains the column names. 
If set to false (the default), Spark treats the first row as data and assigns generic names like _c0, _c1.
.option("inferSchema", "true"): Tells Spark to automatically detect the data types 
(e.g., Integer, Double, Boolean) for each column.

'''

def load_all(raw_dir):
    spark = get_spark()
    dataframes = {}

    for table_name, candidates in _FILE_CANDIDATES.items():
        path = _find_file(raw_dir, candidates)
        if path is None:
            print(f"[WARN] {table_name}: no matching file found in {raw_dir}")
            continue
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        count = df.count()
        print(f"Loaded {table_name}: {count:,} rows  ({os.path.basename(path)})")
        dataframes[table_name] = df

    return dataframes


if __name__ == "__main__":
    import sys
    raw_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    load_all(raw_dir)
