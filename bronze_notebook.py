from pyspark.sql.functions import col, to_timestamp, max

# =========================================================
# 1. READ ORIGINAL BRONZE FILE
# =========================================================

original_path = "abfss://synasstorage@synapsesgacc.dfs.core.windows.net/bronze/nicolustesla/azuresynapseendtoend/refs/heads/main/retail_transactions_bronze.parquet"

df_original = spark.read.parquet(original_path)


# =========================================================
# 2. READ NEW INCREMENTAL FILE
# =========================================================

new_path = "abfss://synasstorage@synapsesgacc.dfs.core.windows.net/bronze/nicolustesla/azuresynapseendtoend/refs/heads/main/retail_incremental_batch_2024_06.parquet"

df_new = spark.read.parquet(new_path)


# =========================================================
# 3. MAKE BOTH FILES HAVE SAME TIMESTAMP TYPE
# =========================================================

df_original = df_original.withColumn(
    "event_timestamp",
    col("event_timestamp").cast("string")
)

df_new = df_new.withColumn(
    "event_timestamp",
    col("event_timestamp").cast("string")
)


# =========================================================
# 4. COMBINE OLD + NEW DATA
# =========================================================

df_bronze = df_original.unionByName(df_new)


# =========================================================
# 5. CONVERT TIMESTAMP
# =========================================================

df_bronze = df_bronze.withColumn(
    "event_timestamp",
    to_timestamp("event_timestamp")
)

df_bronze.printSchema()


# =========================================================
# 6. READ EXISTING WATERMARK
# =========================================================

watermark_path = "abfss://synasstorage@synapsesgacc.dfs.core.windows.net/control/watermark/"

watermark_df = spark.read.parquet(watermark_path)

last_processed_timestamp = watermark_df.collect()[0][0]

print("Last processed timestamp:", last_processed_timestamp)


# =========================================================
# 7. GET ONLY NEW RECORDS
# =========================================================

df_incremental = df_bronze.filter(
    col("event_timestamp") > last_processed_timestamp
)

print("New records:", df_incremental.count())

df_incremental.show()
