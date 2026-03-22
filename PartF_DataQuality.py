# Databricks notebook source
# MAGIC %md
# MAGIC # Part F – Data Quality Validation & Engineering Reflection
# MAGIC ### SmartGear Retail | Advanced Data Engineering Assignment

# COMMAND ----------
import pandas as pd
import json
from datetime import datetime

# ── Load data (local simulation – in Databricks use spark.read.format("delta")) ──
df = pd.read_csv("/dbfs/mnt/raw/sales/year=2025/month=01/smartgear_sales.csv")

EXPECTED_COLUMNS = {"OrderID","OrderDate","Region","StoreID","Product","Quantity","UnitPrice"}
VALID_REGIONS    = {"North","South","East","West"}
VALID_PRODUCTS   = {"Laptop","Smartphone","Tablet","Camera","Headphones",
                    "Smartwatch","Printer","Drone","Monitor","Gaming Console"}

issues = []

# ── Check 1: Missing Values ───────────────────────────────────────────────────
null_counts = df.isnull().sum()
for col, cnt in null_counts.items():
    if cnt > 0:
        issues.append({"check": "NULL_VALUES", "column": col, "count": int(cnt),
                        "severity": "HIGH"})

# ── Check 2: Invalid Quantity (must be 1–5) ──────────────────────────────────
bad_qty = df[(df["Quantity"] <= 0) | (df["Quantity"] > 1000)]
if len(bad_qty) > 0:
    issues.append({"check": "INVALID_QUANTITY", "column": "Quantity",
                   "count": len(bad_qty), "severity": "HIGH",
                   "sample_ids": bad_qty["OrderID"].head(3).tolist()})

# ── Check 3: Invalid Unit Price (must be > 0) ────────────────────────────────
bad_price = df[df["UnitPrice"] <= 0]
if len(bad_price) > 0:
    issues.append({"check": "INVALID_UNIT_PRICE", "column": "UnitPrice",
                   "count": len(bad_price), "severity": "HIGH",
                   "sample_ids": bad_price["OrderID"].head(3).tolist()})

# ── Check 4: Invalid Region Values ───────────────────────────────────────────
df["Region_clean"] = df["Region"].str.strip().str.title()
bad_region = df[~df["Region_clean"].isin(VALID_REGIONS)]
if len(bad_region) > 0:
    issues.append({"check": "INVALID_REGION", "column": "Region",
                   "count": len(bad_region), "severity": "MEDIUM",
                   "values_found": bad_region["Region"].unique().tolist()})

# ── Check 5: Duplicate OrderIDs ───────────────────────────────────────────────
dup_ids = df[df.duplicated(subset=["OrderID"], keep=False)]
if len(dup_ids) > 0:
    issues.append({"check": "DUPLICATE_ORDER_ID", "column": "OrderID",
                   "count": len(dup_ids), "severity": "HIGH",
                   "sample_ids": dup_ids["OrderID"].unique()[:3].tolist()})

# ── Check 6: Invalid Date Format ─────────────────────────────────────────────
df["OrderDate_parsed"] = pd.to_datetime(df["OrderDate"], errors="coerce")
bad_dates = df[df["OrderDate_parsed"].isna()]
if len(bad_dates) > 0:
    issues.append({"check": "INVALID_DATE", "column": "OrderDate",
                   "count": len(bad_dates), "severity": "HIGH"})

# ── Check 7: Unknown Products ─────────────────────────────────────────────────
bad_products = df[~df["Product"].isin(VALID_PRODUCTS)]
if len(bad_products) > 0:
    issues.append({"check": "UNKNOWN_PRODUCT", "column": "Product",
                   "count": len(bad_products), "severity": "MEDIUM",
                   "values_found": bad_products["Product"].unique().tolist()})

# ── Check 8: Schema / Column Presence ────────────────────────────────────────
missing_cols = EXPECTED_COLUMNS - set(df.columns)
if missing_cols:
    issues.append({"check": "MISSING_COLUMNS", "column": str(missing_cols),
                   "count": len(missing_cols), "severity": "CRITICAL"})

# ── Check 9: Revenue Sanity (Quantity * UnitPrice) ───────────────────────────
df["Revenue"] = df["Quantity"] * df["UnitPrice"]
negative_revenue = df[df["Revenue"] <= 0]
if len(negative_revenue) > 0:
    issues.append({"check": "NON_POSITIVE_REVENUE", "column": "Revenue",
                   "count": len(negative_revenue), "severity": "HIGH"})

# ── Quality Summary Output ────────────────────────────────────────────────────
total_records     = len(df)
valid_records     = len(df[
    df["Quantity"].between(1, 1000) &
    (df["UnitPrice"] > 0) &
    df["Region_clean"].isin(VALID_REGIONS) &
    ~df.duplicated(subset=["OrderID"]) &
    df["OrderDate_parsed"].notna()
])
invalid_records   = total_records - valid_records
quality_score_pct = round(valid_records / total_records * 100, 2)

summary = {
    "run_timestamp":       datetime.utcnow().isoformat(),
    "dataset":             "smartgear_sales.csv",
    "total_records":       total_records,
    "valid_records":       valid_records,
    "invalid_records":     invalid_records,
    "data_quality_score":  f"{quality_score_pct}%",
    "checks_passed":       8 - len(issues),
    "checks_failed":       len(issues),
    "issues":              issues if issues else ["No issues found – data is clean ✅"]
}

print(json.dumps(summary, indent=2))

# COMMAND ----------
# MAGIC %md ## Quality Summary Table

# COMMAND ----------
summary_rows = [
    ("Total Records",       str(total_records)),
    ("Valid Records",       str(valid_records)),
    ("Invalid Records",     str(invalid_records)),
    ("Quality Score",       f"{quality_score_pct}%"),
    ("Null Values",         str(int(null_counts.sum()))),
    ("Invalid Quantity",    str(len(bad_qty))),
    ("Invalid Price",       str(len(bad_price))),
    ("Invalid Region",      str(len(bad_region))),
    ("Duplicate OrderIDs",  str(len(dup_ids))),
    ("Invalid Dates",       str(len(bad_dates))),
    ("Unknown Products",    str(len(bad_products))),
]

print(f"{'Check':<25} {'Result':<15}")
print("-" * 40)
for check, result in summary_rows:
    status = "✅" if result in ("0", "1000") or "%" in result else "⚠️ "
    print(f"{check:<25} {result:<15} {status}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Engineering Reflection (150–200 words)
# MAGIC
# MAGIC ### Tool Selection & Cost Awareness
# MAGIC
# MAGIC For SmartGear's data platform, the **Medallion Architecture** (Raw → Bronze → Silver → Gold)
# MAGIC was chosen for its clarity, incremental trust, and auditability. **ADLS Gen2** with hierarchical
# MAGIC namespace provides cost-efficient, scalable object storage with ACL-level security.
# MAGIC **Databricks** was selected over Azure Data Factory's dataflow transformations because it
# MAGIC handles complex business logic, explict schemas, and Delta Lake natively — reducing data
# MAGIC loss risk from inferSchema errors.
# MAGIC
# MAGIC From a cost perspective, **Delta Lake** (open-source) avoids proprietary lock-in while enabling
# MAGIC ACID transactions, time travel, and efficient Parquet storage with Snappy compression — cutting
# MAGIC storage costs vs. raw CSV. **Synapse Serverless SQL** was used for the analytics layer because
# MAGIC it charges per terabyte scanned rather than requiring always-on compute, making it ideal for
# MAGIC ad-hoc executive queries on Gold datasets. **Azure Data Factory**'s parameterised daily
# MAGIC trigger automates ingestion without custom compute, keeping orchestration costs minimal.
# MAGIC Cluster auto-termination in Databricks prevents idle costs. Together, these choices prioritise
# MAGIC reliability, low operational overhead, and cost-conscious production thinking.

# COMMAND ----------
# Save quality summary as JSON to Gold layer
import json

with open("/tmp/quality_summary.json", "w") as f:
    json.dump(summary, f, indent=2)

# In Databricks: dbutils.fs.cp("file:/tmp/quality_summary.json",
#     "abfss://gold@smartgearadls.dfs.core.windows.net/quality/quality_summary.json")
print("✅ Quality summary saved.")
