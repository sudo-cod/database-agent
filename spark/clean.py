from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType, IntegerType


def _count(df, label):
    n = df.count()
    print(f"  {label}: {n:,} rows")
    return n


def clean_orders(df):
    before = _count(df, "orders before")
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in timestamp_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(TimestampType()))

    valid_statuses = ["delivered", "shipped", "canceled", "invoiced", "processing", "created", "approved"]
    df = df.filter(F.col("order_status").isin(valid_statuses))
    df = df.filter(F.col("customer_id").isNotNull())
    df = df.filter(
        F.col("order_delivered_customer_date").isNull() |
        (F.col("order_delivered_customer_date") >= F.col("order_purchase_timestamp"))
    )
    _count(df, "orders after")
    return df


def clean_order_items(df):
    before = _count(df, "order_items before")
    df = df.withColumn("price", F.col("price").cast(DoubleType()))
    df = df.withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
    if "shipping_limit_date" in df.columns:
        df = df.withColumn("shipping_limit_date", F.col("shipping_limit_date").cast(TimestampType()))
    df = df.filter(F.col("price") > 0)
    df = df.withColumn("total_item_value", F.col("price") + F.col("freight_value"))
    _count(df, "order_items after")
    return df


def clean_products(df, category_translation):
    before = _count(df, "products before")
    df = df.filter(F.col("product_id").isNotNull())
    df = df.join(
        category_translation.select("product_category_name", "product_category_name_english"),
        on="product_category_name",
        how="left"
    )
    dim_cols = [
        "product_name_lenght", "product_description_lenght", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ]
    for col in dim_cols:
        if col in df.columns:
            df = df.withColumn(col, F.coalesce(F.col(col), F.lit(0)))
    _count(df, "products after")
    return df


def clean_customers(df):
    before = _count(df, "customers before")
    if "customer_zip_code_prefix" in df.columns:
        df = df.withColumnRenamed("customer_zip_code_prefix", "zip_prefix")
    df = df.dropDuplicates(["customer_id"])
    _count(df, "customers after")
    return df


def clean_sellers(df):
    _count(df, "sellers")
    if "seller_zip_code_prefix" in df.columns:
        df = df.withColumnRenamed("seller_zip_code_prefix", "zip_prefix")
    return df


def clean_order_reviews(df):
    before = _count(df, "order_reviews before")
    df = df.withColumn("review_score", F.col("review_score").cast(IntegerType()))
    df = df.withColumn(
        "review_comment_message",
        F.coalesce(F.col("review_comment_message"), F.lit(""))
    )
    for col in ["review_creation_date", "review_answer_timestamp"]:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(TimestampType()))
    _count(df, "order_reviews after")
    return df


def clean_geolocation(df):
    before = _count(df, "geolocation before")
    if "geolocation_zip_code_prefix" in df.columns:
        df = df.withColumnRenamed("geolocation_zip_code_prefix", "zip_prefix")

    df = df.groupBy("zip_prefix").agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng"),
        F.first("geolocation_city", ignorenulls=True).alias("geolocation_city"),
        F.first("geolocation_state", ignorenulls=True).alias("geolocation_state"),
    )
    _count(df, "geolocation after (deduplicated)")
    return df


def clean_order_payments(df):
    before = _count(df, "order_payments before")
    df = df.withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
    df = df.filter(F.col("payment_value") > 0)
    _count(df, "order_payments after")
    return df


def clean_all(raw):
    print("\n=== CLEANING ===")
    cleaned = dict(raw)

    if "category_translation" not in raw:
        raise RuntimeError("category_translation table is missing — cannot clean products")

    cleaned["orders"] = clean_orders(raw["orders"])
    cleaned["order_items"] = clean_order_items(raw["order_items"])
    cleaned["products"] = clean_products(raw["products"], raw["category_translation"])
    cleaned["customers"] = clean_customers(raw["customers"])
    cleaned["sellers"] = clean_sellers(raw["sellers"])
    cleaned["order_reviews"] = clean_order_reviews(raw["order_reviews"])
    cleaned["geolocation"] = clean_geolocation(raw["geolocation"])
    cleaned["order_payments"] = clean_order_payments(raw["order_payments"])

    return cleaned
