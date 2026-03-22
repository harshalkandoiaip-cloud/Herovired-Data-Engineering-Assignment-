# =============================================================================
# PART A – Cloud Storage & Architecture
# SmartGear Retail – Advanced Data Engineering Assignment
# =============================================================================
# Run this in Azure Cloud Shell (Bash) or adapt as ARM template deployment.
# All commands use Azure CLI.

# ─── Step 1: Variables ────────────────────────────────────────────────────────
RESOURCE_GROUP="smartgear-rg"
LOCATION="eastus"
STORAGE_ACCOUNT="smartgearadls"          # must be globally unique, lowercase
SUBSCRIPTION_ID="<your-subscription-id>"

# ─── Step 2: Create Resource Group ───────────────────────────────────────────
az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION

# ─── Step 3: Create ADLS Gen2 Storage Account ────────────────────────────────
# Hierarchical Namespace (HNS) = true  → enables ADLS Gen2 semantics
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hns true \
  --enable-sftp false \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false \
  --default-action Deny \
  --bypass AzureServices Logging Metrics

# ─── Step 4: Enable Security Settings ────────────────────────────────────────
# Enforce HTTPS-only, soft-delete for blobs & containers
az storage account update \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --https-only true

az storage blob service-properties delete-policy update \
  --account-name $STORAGE_ACCOUNT \
  --enable true \
  --days-retained 7

# ─── Step 5: Create Medallion Containers (Filesystems) ───────────────────────
for CONTAINER in raw bronze silver gold; do
  az storage fs create \
    --name $CONTAINER \
    --account-name $STORAGE_ACCOUNT \
    --auth-mode login
done

# ─── Step 6: Create Year/Month Partition Folder Structure ────────────────────
# Pattern: /<container>/sales/year=YYYY/month=MM/
for CONTAINER in raw bronze silver gold; do
  for YEAR in 2025; do
    for MONTH in 01 02 03 04; do
      az storage fs directory create \
        --name "sales/year=${YEAR}/month=${MONTH}" \
        --file-system $CONTAINER \
        --account-name $STORAGE_ACCOUNT \
        --auth-mode login
    done
  done
done

# ─── Step 7: Upload Raw Dataset ──────────────────────────────────────────────
# Upload CSV to raw/sales/year=2025/month=01/ (adjust month as needed)
az storage fs file upload \
  --source "./smartgear_sales.csv" \
  --path "sales/year=2025/month=01/smartgear_sales.csv" \
  --file-system raw \
  --account-name $STORAGE_ACCOUNT \
  --auth-mode login

echo "✅ PART A COMPLETE – ADLS Gen2 set up with medallion containers and partitioned structure."

# ─── Folder Structure Reference ──────────────────────────────────────────────
# raw/
#   sales/
#     year=2025/
#       month=01/  ← smartgear_sales.csv uploaded here
#       month=02/
#       month=03/
#       month=04/
#
# bronze/  silver/  gold/  → same structure, populated by ADF & Databricks
