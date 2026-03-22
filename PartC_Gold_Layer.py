# Databricks notebook source
# MAGIC %md
# MAGIC # Part C – Gold Executive Layer
# MAGIC ### SmartGear Retail | Advanced Data Engineering Assignment

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SmartGear_Gold").getOrCreate()

STORAGE_ACCOUNT  = "smartgearadls"
CONTAINER_SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CONTAINER_GOLD   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

SILVER_PATH = f"{CONTAINER_SILVER}/sales"
GOLD_REGION_KPI_PATH  = f"{CONTAINER_GOLD}/region_kpi"
GOLD_TOP5_PATH        = f"{CONTAINER_GOLD}/top5_products"
GOLD_STORE_PERF_PATH  = f"{CONTAINER_GOLD}/store_performance"

df_silver = spark.read.format("delta").load(SILVER_PATH)

# COMMAND ----------
# MAGIC %md ## 1. Region-Level KPI Dataset

# COMMAND ----------
df_region_kpi = (
    df_silver
    .groupBy("Region")
    .agg(
        F.sum("Revenue").alias("TotalRevenue"),
        F.count("OrderID").alias("TotalOrders"),
        F.sum("Quantity").alias("TotalUnitsSold"),
        F.avg("Revenue").alias("AvgOrderValue"),
        F.max("Revenue").alias("MaxOrderValue"),
        F.min("Revenue").alias("MinOrderValue"),
    )
    .withColumn("TotalRevenue",  F.round("TotalRevenue",  2))
    .withColumn("AvgOrderValue", F.round("AvgOrderValue", 2))
    .withColumn("MaxOrderValue", F.round("MaxOrderValue", 2))
    .withColumn("MinOrderValue", F.round("MinOrderValue", 2))
    .orderBy(F.desc("TotalRevenue"))
)

df_region_kpi.show()

(
    df_region_kpi
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_REGION_KPI_PATH)
)
print(f"✅ Region KPI written: {GOLD_REGION_KPI_PATH}")

# COMMAND ----------
# MAGIC %md ## 2. Top 5 Products by Revenue

# COMMAND ----------
df_top5 = (
    df_silver
    .groupBy("Product")
    .agg(
        F.sum("Revenue").alias("TotalRevenue"),
        F.sum("Quantity").alias("TotalUnitsSold"),
        F.count("OrderID").alias("TotalOrders"),
        F.avg("UnitPrice").alias("AvgUnitPrice"),
    )
    .withColumn("TotalRevenue",  F.round("TotalRevenue",  2))
    .withColumn("AvgUnitPrice",  F.round("AvgUnitPrice",  2))
    .orderBy(F.desc("TotalRevenue"))
    .limit(5)
    .withColumn("Rank", F.monotonically_increasing_id() + 1)
)

df_top5.show()

(
    df_top5
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_TOP5_PATH)
)
print(f"✅ Top 5 Products written: {GOLD_TOP5_PATH}")

# COMMAND ----------
# MAGIC %md ## 3. Store-Level Performance Dataset

# COMMAND ----------
df_store = (
    df_silver
    .groupBy("StoreID", "Region")
    .agg(
        F.sum("Revenue").alias("TotalRevenue"),
        F.count("OrderID").alias("TotalOrders"),
        F.sum("Quantity").alias("TotalUnitsSold"),
        F.avg("Revenue").alias("AvgOrderValue"),
        F.countDistinct("Product").alias("UniqueProducts"),
    )
    .withColumn("TotalRevenue",  F.round("TotalRevenue",  2))
    .withColumn("AvgOrderValue", F.round("AvgOrderValue", 2))
    .orderBy(F.desc("TotalRevenue"))
)

df_store.show(20)

(
    df_store
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("Region")
    .option("overwriteSchema", "true")
    .save(GOLD_STORE_PERF_PATH)
)
print(f"✅ Store Performance written: {GOLD_STORE_PERF_PATH}")

# COMMAND ----------
# MAGIC %md ## 4. Verification – Gold Layer Summary

# COMMAND ----------
print("=== GOLD LAYER SUMMARY ===")
print(f"Region KPI rows : {spark.read.format('delta').load(GOLD_REGION_KPI_PATH).count()}")
print(f"Top 5 rows      : {spark.read.format('delta').load(GOLD_TOP5_PATH).count()}")
print(f"Store perf rows : {spark.read.format('delta').load(GOLD_STORE_PERF_PATH).count()}")
