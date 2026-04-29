from pyspark.sql import functions as F


def enrich_orders(orders):
    orders = orders.withColumn(
        "delivery_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull(),
            F.datediff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp"))
        ).otherwise(None)
    )
    orders = orders.withColumn(
        "is_late",
        F.when(
            F.col("order_delivered_customer_date").isNotNull() &
            F.col("order_estimated_delivery_date").isNotNull(),
            F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date")
        ).otherwise(None)
    )
    return orders


def enrich_order_items(order_items, products):
    category_cols = products.select("product_id", "product_category_name_english")
    return order_items.join(category_cols, on="product_id", how="left")


def enrich_customers(customers, geolocation):
    geo = geolocation.select(
        "zip_prefix",
        F.col("geolocation_lat").alias("customer_lat"),
        F.col("geolocation_lng").alias("customer_lng"),
    )
    return customers.join(geo, on="zip_prefix", how="left")


def enrich_sellers(sellers, geolocation):
    geo = geolocation.select(
        "zip_prefix",
        F.col("geolocation_lat").alias("seller_lat"),
        F.col("geolocation_lng").alias("seller_lng"),
    )
    return sellers.join(geo, on="zip_prefix", how="left")


def build_order_summary(orders, customers, order_payments, order_items, order_reviews):
    # Aggregate payments: total value and most common payment type per order
    payments_agg = order_payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("total_payment"),
        F.first("payment_type", ignorenulls=True).alias("payment_type"),
    )

    # Aggregate items: count and total value per order
    items_agg = order_items.groupBy("order_id").agg(
        F.count("*").alias("item_count"),
        F.sum("total_item_value").alias("total_items_value"),
    )

    # Aggregate reviews: max score per order
    reviews_agg = order_reviews.groupBy("order_id").agg(
        F.max("review_score").alias("review_score")
    )

    # Customer columns
    customers_slim = customers.select(
        "customer_id",
        "customer_city",
        "customer_state",
    )

    # Build purchase_date (date only)
    orders_base = orders.withColumn(
        "purchase_date", F.to_date(F.col("order_purchase_timestamp"))
    ).select(
        "order_id", "customer_id", "order_status",
        "purchase_date", "delivery_days", "is_late"
    )

    summary = (
        orders_base
        .join(customers_slim, on="customer_id", how="left")
        .join(payments_agg, on="order_id", how="left")
        .join(items_agg, on="order_id", how="left")
        .join(reviews_agg, on="order_id", how="left")
    )
    return summary


def enrich_all(cleaned):
    print("\n=== ENRICHING ===")
    enriched = dict(cleaned)

    enriched["orders"] = enrich_orders(cleaned["orders"])
    print("orders: delivery_days + is_late added")

    enriched["order_items"] = enrich_order_items(cleaned["order_items"], cleaned["products"])
    print("order_items: category_name_english joined")

    enriched["customers"] = enrich_customers(cleaned["customers"], cleaned["geolocation"])
    print("customers: lat/lng joined")

    enriched["sellers"] = enrich_sellers(cleaned["sellers"], cleaned["geolocation"])
    print("sellers: lat/lng joined")

    enriched["order_summary"] = build_order_summary(
        enriched["orders"],
        enriched["customers"],
        cleaned["order_payments"],
        enriched["order_items"],
        cleaned["order_reviews"],
    )
    count = enriched["order_summary"].count()
    print(f"order_summary: {count:,} rows")

    return enriched
