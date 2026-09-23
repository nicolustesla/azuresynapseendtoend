from pyspark.sql.functions import col, to_date, lower, max

# Read Bronze
df_bronze = spark.read.parquet(
    "abfss://synasstorage@synapsesgacc.dfs.core.windows.net/bronze/nicolustesla/azuresynapseendtoend/refs/heads/main/retail_transactions_bronze.parquet"
)

# Check if Silver already exists
silver_path = "abfss://synasstorage@synapsesgacc.dfs.core.windows.net/silver/"

try:
    df_existing = spark.read.parquet(silver_path)
    last_timestamp = df_existing.agg(
        max("event_timestamp")
    ).collect()[0][0]
except:
    last_timestamp = None

# Incremental filter
if last_timestamp is not None:
    df_bronze = df_bronze.filter(
        col("event_timestamp") > last_timestamp
    )

# Clean data
df_silver = (
    df_bronze
    .filter(col("event_type") == "purchase")
    .dropna(subset=["customer_id", "amount"])
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("payment_method", lower(col("payment_method")))
    .withColumn("amount", col("amount").cast("float"))
    .select(
        "event_id",
        "customer_id",
        "event_timestamp",
        "event_date",
        "product_id",
        "product_category",
        "payment_method",
        "amount",
        "location"
    )
)

df_silver.show()

# Append new records
df_silver.write.mode("append").parquet(silver_path)
