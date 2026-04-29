import duckdb
import os


def load_warehouse(
    processed_dir: str = None,
    db_path: str = None
):
    base = os.path.join(os.path.dirname(__file__), '..')
    processed_dir = processed_dir or os.path.join(base, 'data', 'processed')
    db_path = db_path or os.path.join(base, 'data', 'olist.duckdb')

    con = duckdb.connect(db_path)
    tables = [
        "orders", "order_items", "order_payments",
        "order_reviews", "products", "customers",
        "sellers", "order_summary"
    ]

    for table in tables:
        parquet_path = os.path.join(processed_dir, table, "*.parquet")
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{parquet_path}')")
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"Loaded {table}: {count:,} rows")

    con.close()
    print(f"Warehouse ready: {db_path}")


def verify_warehouse(db_path: str = None):
    base = os.path.join(os.path.dirname(__file__), '..')
    db_path = db_path or os.path.join(base, 'data', 'olist.duckdb')
    con = duckdb.connect(db_path, read_only=True)
    result = con.execute("""
        SELECT COUNT(*) as joined_rows
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
    """).fetchone()
    print(f"Sanity check — orders JOIN order_items: {result[0]:,} rows")
    con.close()


if __name__ == "__main__":
    load_warehouse()
    verify_warehouse()
