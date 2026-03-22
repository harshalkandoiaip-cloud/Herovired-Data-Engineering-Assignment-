# Databricks notebook source
# MAGIC %md
# MAGIC # Part B – Bronze & Silver Layers
# MAGIC ### SmartGear Retail | Advanced Data Engineering Assignment

# COMMAND ----------
# MAGIC %md ## 1. Configuration & Constants

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)
from datetime import datetime

spark = SparkSession.builder.appName("SmartGear_Bronze_Silver").getOrCreate()

STORAGE_ACCOUNT = "smartgearadls"
CONTAINER_RAW    = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CONTAINER_BRONZE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CONTAINER_SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"

RAW_PATH    = f"{CONTAINER_RAW}/sales/year=2025/month=01/smartgear_sales.csv"
BRONZE_PATH = f"{CONTAINER_BRONZE}/sales"
SILVER_PATH = f"{CONTAINER_SILVER}/sales"

INGESTION_TS = datetime.utcnow().isoformat()

# COMMAND ----------
# MAGIC %md ## 2. Bronze Layer – Ingest Raw CSV with Explicit Schema

# COMMAND ----------
# ── Explicit schema (no inferSchema) ─────────────────────────────────────────
bronze_schema = StructType([
    StructField("OrderID",    IntegerType(), nullable=True),
    StructField("OrderDate",  DateType(),    nullable=True),
    StructField("Region",     StringType(),  nullable=True),
    StructField("StoreID",    IntegerType(), nullable=True),
    StructField("Product",    StringType(),  nullable=True),
    StructField("Quantity",   IntegerType(), nullable=True),
    StructField("UnitPrice",  DoubleType(),  nullable=True),
])

df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")          # captures malformed rows
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(bronze_schema)
    .load(RAW_PATH)
)

print(f"Raw record count: {df_raw.count()}")
df_raw.printSchema()
df_raw.show(5, truncate=False)

# COMMAND ----------
# MAGIC %md ### 2a. Handle Nulls & Invalid Records

# COMMAND ----------
# ── Separate invalid records ──────────────────────────────────────────────────
df_invalid = df_raw.filter(
    F.col("OrderID").isNull()   |
    F.col("OrderDate").isNull() |
    F.col("Region").isNull()    |
    F.col("Product").isNull()   |
    F.col("Quantity").isNull()  | (F.col("Quantity") <= 0) |
    F.col("UnitPrice").isNull() | (F.col("UnitPrice") <= 0)
)

df_valid = df_raw.filter(
    F.col("OrderID").isNotNull()   &
    F.col("OrderDate").isNotNull() &
    F.col("Region").isNotNull()    &
    F.col("Product").isNotNull()   &
    F.col("Quantity").isNotNull()  & (F.col("Quantity") > 0) &
    F.col("UnitPrice").isNotNull() & (F.col("UnitPrice") > 0)
)

print(f"Valid records  : {df_valid.count()}")
print(f"Invalid records: {df_invalid.count()}")

# COMMAND ----------
# MAGIC %md ### 2b. Add Ingestion Metadata Columns

# COMMAND ----------
df_bronze = (
    df_valid
    .withColumn("ingestion_timestamp", F.lit(INGESTION_TS).cast("timestamp"))
    .withColumn("ingestion_source",    F.lit(RAW_PATH))
    .withColumn("year",  F.year("OrderDate").cast("string"))
    .withColumn("month", F.lpad(F.month("OrderDate").cast("string"), 2, "0"))
    .withColumn("record_status", F.lit("VALID"))
)

df_bronze.printSchema()
df_bronze.show(3, truncate=False)

# COMMAND ----------
# MAGIC %md ### 2c. Write Bronze Layer (Parquet, partitioned by year/month)

# COMMAND ----------
(
    df_bronze
    .write
    .format("delta")              # Delta Lake for ACID guarantees
    .mode("overwrite")
    .partitionBy("year", "month")
    .option("overwriteSchema", "true")
    .save(BRONZE_PATH)
)

print(f"✅ Bronze written to: {BRONZE_PATH}")
spark.read.format("delta").load(BRONZE_PATH).groupBy("year","month").count().show()

# COMMAND ----------
# MAGIC %md ## 3. Silver Layer – Cleaned & Enriched Data

# COMMAND ----------
df_b = spark.read.format("delta").load(BRONZE_PATH)

# ── Revenue column ────────────────────────────────────────────────────────────
df_silver = df_b.withColumn("Revenue", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))

# ── Standardize region values (Title-case) ────────────────────────────────────
df_silver = df_silver.withColumn(
    "Region",
    F.initcap(F.trim(F.col("Region")))
)

# ── Remove duplicate OrderIDs (keep first) ────────────────────────────────────
from pyspark.sql.window import Window
w = Window.partitionBy("OrderID").orderBy("ingestion_timestamp")
df_silver = (
    df_silver
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print(f"Silver record count: {df_silver.count()}")
df_silver.show(5)

# COMMAND ----------
# MAGIC %md ### 3a. Product-Level Summary

# COMMAND ----------
df_product_summary = (
    df_silver
    .groupBy("Product")
    .agg(
        F.sum("Revenue").alias("TotalRevenue"),
        F.sum("Quantity").alias("TotalQuantity"),
        F.count("OrderID").alias("TotalOrders"),
        F.avg("UnitPrice").alias("AvgUnitPrice"),
    )
    .withColumn("TotalRevenue", F.round("TotalRevenue", 2))
    .withColumn("AvgUnitPrice", F.round("AvgUnitPrice", 2))
    .orderBy(F.desc("TotalRevenue"))
)

df_product_summary.show(truncate=False)

# COMMAND ----------
# MAGIC %md ### 3b. Regional Summary

# COMMAND ----------
df_region_summary = (
    df_silver
    .groupBy("Region")
    .agg(
        F.sum("Revenue").alias("TotalRevenue"),
        F.sum("Quantity").alias("TotalQuantity"),
        F.count("OrderID").alias("TotalOrders"),
        F.avg("Revenue").alias("AvgOrderValue"),
    )
    .withColumn("TotalRevenue",  F.round("TotalRevenue", 2))
    .withColumn("AvgOrderValue", F.round("AvgOrderValue", 2))
    .orderBy(F.desc("TotalRevenue"))
)

df_region_summary.show()

# COMMAND ----------
# MAGIC %md ### 3c. Write Silver Layer

# COMMAND ----------
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("year", "month")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)

# Also write summaries alongside silver
SILVER_PRODUCT_PATH = f"{CONTAINER_SILVER}/summaries/product"
SILVER_REGION_PATH  = f"{CONTAINER_SILVER}/summaries/region"

df_product_summary.write.format("delta").mode("overwrite").save(SILVER_PRODUCT_PATH)
df_region_summary.write.format("delta").mode("overwrite").save(SILVER_REGION_PATH)

print(f"✅ Silver layer written to: {SILVER_PATH}")
print(f"✅ Product summary: {SILVER_PRODUCT_PATH}")
print(f"✅ Region summary : {SILVER_REGION_PATH}")
