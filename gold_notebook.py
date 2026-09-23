# Gold Layer - Daily Revenue + Fact Sales

from pyspark.sql.functions import sum, count

# Read Silver data
df_silver = spark.read.parquet(
    "abfss://synasstorage@synapsesgacc.dfs.core.windows.net/silver/"
)

# =========================================================
# 1. DAILY REVENUE
# =========================================================

df_daily_revenue = (
    df_silver
    .groupBy("event_date")
    .agg(
        sum("amount").alias("daily_revenue"),
        count("*").alias("total_purchases")
    )
)

df_daily_revenue.show()

# Write Daily Revenue
df_daily_revenue.write.mode("overwrite").parquet(
    "abfss://synasstorage@synapsesgacc.dfs.core.windows.net/gold/daily_revenue/"
)


# =========================================================
# 2. FACT SALES
# =========================================================

df_fact_sales = (
    df_silver
    .groupBy(
        "event_date",
        "customer_id",
        "product_id",
        "product_category",
        "location",
        "payment_method"
    )
    .agg(
        sum("amount").alias("sales_amount"),
        count("*").alias("quantity")
    )
)

df_fact_sales.show()

# Write Fact Sales
df_fact_sales.write.mode("overwrite").parquet(
    "abfss://synasstorage@synapsesgacc.dfs.core.windows.net/gold/fact_sales/"
)
