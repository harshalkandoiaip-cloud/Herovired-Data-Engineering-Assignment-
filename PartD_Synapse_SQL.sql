-- =============================================================================
-- PART D – Synapse Serverless Analytics
-- SmartGear Retail | Advanced Data Engineering Assignment
-- Run these scripts in Azure Synapse Analytics → Develop → SQL Script
-- Serverless SQL Pool (Built-in)
-- =============================================================================

-- ─── 0. Prerequisites: Credential & Data Source ───────────────────────────────
-- Create a database-scoped credential using a Managed Identity
-- (Run once per Synapse workspace)

IF NOT EXISTS (SELECT * FROM sys.credentials WHERE name = 'smartgear_managed_identity')
CREATE CREDENTIAL [smartgear_managed_identity]
WITH IDENTITY = 'Managed Identity';
GO

-- Create external data source pointing to Gold container
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'smartgear_gold')
CREATE EXTERNAL DATA SOURCE [smartgear_gold]
WITH (
    LOCATION   = 'abfss://gold@smartgearadls.dfs.core.windows.net',
    CREDENTIAL = [smartgear_managed_identity]
);
GO

-- ─── 1. External Table – Region KPI ──────────────────────────────────────────
-- Drop if exists, then recreate
IF OBJECT_ID('dbo.ext_region_kpi', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_region_kpi;
GO

CREATE EXTERNAL TABLE dbo.ext_region_kpi (
    Region         NVARCHAR(50),
    TotalRevenue   FLOAT,
    TotalOrders    BIGINT,
    TotalUnitsSold BIGINT,
    AvgOrderValue  FLOAT,
    MaxOrderValue  FLOAT,
    MinOrderValue  FLOAT
)
WITH (
    LOCATION     = 'region_kpi/**',
    DATA_SOURCE  = smartgear_gold,
    FILE_FORMAT  = SynapseParquetFormat          -- built-in Parquet format
);
GO

-- ─── 2. External Table – Top 5 Products ──────────────────────────────────────
IF OBJECT_ID('dbo.ext_top5_products', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_top5_products;
GO

CREATE EXTERNAL TABLE dbo.ext_top5_products (
    Product        NVARCHAR(100),
    TotalRevenue   FLOAT,
    TotalUnitsSold BIGINT,
    TotalOrders    BIGINT,
    AvgUnitPrice   FLOAT,
    Rank           BIGINT
)
WITH (
    LOCATION     = 'top5_products/**',
    DATA_SOURCE  = smartgear_gold,
    FILE_FORMAT  = SynapseParquetFormat
);
GO

-- ─── 3. External Table – Store Performance ───────────────────────────────────
IF OBJECT_ID('dbo.ext_store_performance', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_store_performance;
GO

CREATE EXTERNAL TABLE dbo.ext_store_performance (
    StoreID        INT,
    Region         NVARCHAR(50),
    TotalRevenue   FLOAT,
    TotalOrders    BIGINT,
    TotalUnitsSold BIGINT,
    AvgOrderValue  FLOAT,
    UniqueProducts BIGINT
)
WITH (
    LOCATION     = 'store_performance/**',
    DATA_SOURCE  = smartgear_gold,
    FILE_FORMAT  = SynapseParquetFormat
);
GO

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================

-- ─── Q1. Total Revenue by Region ─────────────────────────────────────────────
SELECT
    Region,
    TotalRevenue,
    TotalOrders,
    TotalUnitsSold,
    ROUND(TotalRevenue / SUM(TotalRevenue) OVER () * 100, 2) AS RevenueSharePct
FROM dbo.ext_region_kpi
ORDER BY TotalRevenue DESC;

-- ─── Q2. Top 5 Products by Revenue ───────────────────────────────────────────
SELECT
    Rank,
    Product,
    TotalRevenue,
    TotalUnitsSold,
    AvgUnitPrice
FROM dbo.ext_top5_products
ORDER BY Rank;

-- ─── Q3. Top 10 Stores by Revenue ────────────────────────────────────────────
SELECT TOP 10
    StoreID,
    Region,
    TotalRevenue,
    TotalOrders,
    AvgOrderValue
FROM dbo.ext_store_performance
ORDER BY TotalRevenue DESC;

-- ─── Q4. Grand Total Revenue Validation ──────────────────────────────────────
SELECT
    SUM(TotalRevenue)   AS GrandTotalRevenue,
    SUM(TotalOrders)    AS GrandTotalOrders,
    SUM(TotalUnitsSold) AS GrandTotalUnits,
    AVG(AvgOrderValue)  AS OverallAvgOrderValue
FROM dbo.ext_region_kpi;

-- ─── Q5. Revenue Concentration (Top Store per Region) ────────────────────────
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Region ORDER BY TotalRevenue DESC) AS rn
    FROM dbo.ext_store_performance
)
SELECT
    Region,
    StoreID           AS TopStoreID,
    TotalRevenue      AS TopStoreRevenue,
    TotalOrders       AS TopStoreOrders
FROM ranked
WHERE rn = 1
ORDER BY TotalRevenue DESC;

-- ─── Q6. OPENROWSET – Direct Delta Parquet Query (no external table needed) ──
-- Alternative approach: query files directly without creating external tables
SELECT TOP 5 *
FROM OPENROWSET(
    BULK       'gold/region_kpi/**',
    DATA_SOURCE = 'smartgear_gold',
    FORMAT     = 'PARQUET'
) AS kpi
ORDER BY TotalRevenue DESC;
