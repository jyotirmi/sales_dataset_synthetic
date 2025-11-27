# Sales Analytics Demo Dataset - Complete Setup Guide

## 📋 Overview

This package provides a complete synthetic dataset with **62 tables** and **~1.5 million records** for building sales analytics demos in Snowflake or Databricks.

### Dataset Includes:
- **Sales & Customer Data** (17 tables): Customers, products, opportunities, deals, contracts, quotes
- **Product Usage & Telemetry** (11 tables): Usage metrics, feature adoption, API calls, health alerts
- **Marketing Data** (13 tables): Campaigns, events, leads, engagement, attribution, content
- **Support & Service** (12 tables): Tickets, resolutions, CSAT, SLA tracking, knowledge base
- **Operational Data** (9 tables): Health scores, invoices, payments, renewals, audit logs

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- pandas, numpy
- AWS S3 bucket (recommended) OR local storage
- Snowflake account OR Databricks workspace

### Step 1: Generate the Data

```bash
# Run all FOUR generator scripts in order
python sales_data_generator_part1.py
python sales_data_generator_part2.py
python sales_data_generator_part3.py
python sales_data_generator_part4.py  # NEW - Generates remaining 15 tables

# Verify all 62 tables were generated
python verify_dataset.py
```

**Output**: `sales_analytics_data/` directory with 62 CSV files organized by schema

### What Each Part Generates:

**Part 1** (16 tables): Core sales tables - customers, products, opportunities, deals, campaigns, leads, tickets
**Part 2** (10 tables): Extended sales, usage metrics, integrations, quotes, contracts  
**Part 3** (21 tables): Marketing details, support details, telemetry, operational data
**Part 4** (15 tables): **NEW** - Revenue recognition, territories, knowledge base, escalations, onboarding, releases, user accounts, payments, renewals

### Step 2: Upload to S3 (Recommended)

```bash
# Using AWS CLI
aws s3 sync sales_analytics_data/ s3://your-bucket/sales-demo-data/

# Verify upload
aws s3 ls s3://your-bucket/sales-demo-data/ --recursive | wc -l
# Should show 62 files
```

### Step 3: Load into Your Platform

Choose **Option A** (Snowflake) OR **Option B** (Databricks)

---

## ❄️ Option A: Loading into Snowflake

### Method 1: Using Snowflake Web UI + SQL

1. **Upload DDL script to Snowflake worksheet**
   - Open Snowflake Web UI
   - Go to Worksheets → Create new worksheet
   - Copy/paste the `snowflake_ddl_script.sql`
   - Run sections sequentially

2. **Create S3 External Stage**
   ```sql
   CREATE OR REPLACE STAGE sales_analytics_demo.public.s3_stage
     URL = 's3://your-bucket/sales-demo-data/'
     CREDENTIALS = (AWS_KEY_ID = 'xxx' AWS_SECRET_KEY = 'xxx')
     FILE_FORMAT = sales_analytics_demo.public.csv_format;
   ```

3. **Load data using COPY commands**
   ```sql
   -- Example for customers table
   COPY INTO sales_analytics_demo.sales_data.customers
   FROM @sales_analytics_demo.public.s3_stage/sales_data/customers.csv
   FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format')
   ON_ERROR = 'CONTINUE';
   
   -- Repeat for all 62 tables
   ```

4. **Verify data**
   ```sql
   SELECT COUNT(*) FROM sales_analytics_demo.sales_data.customers;
   SELECT COUNT(*) FROM sales_analytics_demo.support_data.support_tickets;
   ```

### Method 2: Using Python + Snowflake Connector

```python
import snowflake.connector
import pandas as pd
import glob

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='YOUR_USERNAME',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',
    warehouse='COMPUTE_WH',
    database='sales_analytics_demo'
)

# Load all CSV files
csv_files = glob.glob('sales_analytics_data/**/*.csv', recursive=True)

for csv_file in csv_files:
    # Parse schema and table name from path
    parts = csv_file.split('/')
    schema = parts[1].replace('_data', '')  # e.g., 'sales_data' -> 'sales'
    table = parts[2].replace('.csv', '')
    
    # Read CSV
    df = pd.read_csv(csv_file)
    
    # Write to Snowflake
    from snowflake.connector.pandas_tools import write_pandas
    
    write_pandas(
        conn=conn,
        df=df,
        table_name=table.upper(),
        database='sales_analytics_demo',
        schema=f'{schema}_data'
    )
    
    print(f"✓ Loaded {schema}.{table}: {len(df)} rows")

conn.close()
```

### Method 3: Using SnowSQL (Command Line)

```bash
# Upload files to internal stage
snowsql -a your_account -u your_user -d sales_analytics_demo << EOF
USE WAREHOUSE COMPUTE_WH;
USE DATABASE sales_analytics_demo;

-- Upload all files
PUT file://sales_analytics_data/sales_data/*.csv @public.csv_stage/sales_data/;
PUT file://sales_analytics_data/usage_data/*.csv @public.csv_stage/usage_data/;
PUT file://sales_analytics_data/marketing_data/*.csv @public.csv_stage/marketing_data/;
PUT file://sales_analytics_data/support_data/*.csv @public.csv_stage/support_data/;
PUT file://sales_analytics_data/operational_data/*.csv @public.csv_stage/operational_data/;

-- Load from stage
COPY INTO sales_data.customers FROM @public.csv_stage/sales_data/customers.csv;
-- ... repeat for all tables
EOF
```

---

## 🧱 Option B: Loading into Databricks

### Method 1: Using Databricks UI

1. **Upload the Databricks notebook**
   - Open Databricks workspace
   - Import the `databricks_loader.py` as a notebook
   - Attach to a cluster

2. **Configure S3 access**
   ```python
   # In notebook cell
   S3_BUCKET = "your-bucket-name"
   S3_PATH = f"s3a://{S3_BUCKET}/sales-demo-data/"
   
   # Set credentials (or use IAM role on cluster)
   spark.conf.set("fs.s3a.access.key", "YOUR_KEY")
   spark.conf.set("fs.s3a.secret.key", "YOUR_SECRET")
   ```

3. **Run all cells**
   - Creates catalog/schemas
   - Loads all 62 tables into Delta format
   - Runs validation queries

4. **Verify data**
   ```python
   spark.sql("SELECT COUNT(*) FROM sales_analytics.sales_data.customers").show()
   spark.sql("SHOW TABLES IN sales_analytics.sales_data").show()
   ```

### Method 2: Using Databricks CLI

```bash
# Upload notebook
databricks workspace import databricks_loader.py /Users/your_email/sales_demo_loader

# Create job to run notebook
databricks jobs create --json '{
  "name": "Load Sales Demo Data",
  "tasks": [{
    "task_key": "load_data",
    "notebook_task": {
      "notebook_path": "/Users/your_email/sales_demo_loader"
    },
    "existing_cluster_id": "your-cluster-id"
  }]
}'

# Run job
databricks jobs run-now --job-id YOUR_JOB_ID
```

### Method 3: PySpark Script (Standalone)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Sales Demo Loader") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configure S3
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "YOUR_KEY")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "YOUR_SECRET")

# Load example table
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3a://your-bucket/sales-demo-data/sales_data/customers.csv")

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_analytics.sales_data.customers")
```

---

## 📊 Sample Queries for Your Demo

### Sales Performance

```sql
-- Quarterly sales performance
SELECT 
    quarter,
    status,
    COUNT(*) as deal_count,
    SUM(deal_value) as total_revenue,
    AVG(sales_cycle_days) as avg_sales_cycle
FROM sales_analytics_demo.sales_data.closed_deals
WHERE fiscal_year = 2024
GROUP BY quarter, status
ORDER BY quarter;

-- Win rate by region and product
SELECT 
    cd.region,
    p.product_name,
    COUNT(CASE WHEN cd.status = 'Won' THEN 1 END) as won_count,
    COUNT(CASE WHEN cd.status = 'Lost' THEN 1 END) as lost_count,
    ROUND(COUNT(CASE WHEN cd.status = 'Won' THEN 1 END) * 100.0 / COUNT(*), 2) as win_rate_pct
FROM sales_analytics_demo.sales_data.closed_deals cd
JOIN sales_analytics_demo.sales_data.products p ON cd.product_id = p.product_id
GROUP BY cd.region, p.product_name
ORDER BY win_rate_pct DESC;
```

### Customer Expansion Opportunities

```sql
-- Find similar customers to ACME (for upsell)
WITH acme_profile AS (
    SELECT industry, company_size, region
    FROM sales_analytics_demo.sales_data.customers
    WHERE customer_name LIKE '%ACME%'
    LIMIT 1
)
SELECT 
    c.customer_name,
    c.industry,
    c.company_size,
    COUNT(DISTINCT cp.product_id) as products_owned,
    SUM(cp.contract_value) as total_contract_value
FROM sales_analytics_demo.sales_data.customers c
JOIN sales_analytics_demo.sales_data.customer_products cp ON c.customer_id = cp.customer_id
CROSS JOIN acme_profile ap
WHERE c.industry = ap.industry
    AND c.company_size = ap.company_size
    AND c.region = ap.region
GROUP BY c.customer_name, c.industry, c.company_size
HAVING COUNT(DISTINCT cp.product_id) < 3  -- Opportunity for more products
ORDER BY total_contract_value DESC
LIMIT 20;
```

### Product Usage & Health

```sql
-- Customers at risk (low usage + high support tickets)
SELECT 
    c.customer_name,
    c.customer_tier,
    AVG(pu.license_utilization) as avg_license_utilization,
    COUNT(DISTINCT st.ticket_id) as ticket_count,
    AVG(CASE WHEN pu.health_score = 'Critical' THEN 1
             WHEN pu.health_score = 'At Risk' THEN 0.5
             ELSE 0 END) as health_risk_score
FROM sales_analytics_demo.sales_data.customers c
LEFT JOIN sales_analytics_demo.usage_data.product_usage_summary pu 
    ON c.customer_id = pu.customer_id
LEFT JOIN sales_analytics_demo.support_data.support_tickets st 
    ON c.customer_id = st.customer_id
WHERE pu.usage_date >= CURRENT_DATE - 90
GROUP BY c.customer_name, c.customer_tier
HAVING avg_license_utilization < 50 
    OR ticket_count > 10
ORDER BY health_risk_score DESC, ticket_count DESC
LIMIT 25;
```

### Marketing Campaign ROI

```sql
-- Campaign attribution and ROI
SELECT 
    c.campaign_name,
    c.campaign_type,
    c.budget,
    COUNT(DISTINCT l.lead_id) as leads_generated,
    COUNT(DISTINCT ma.opportunity_id) as opportunities_created,
    COUNT(DISTINCT cd.deal_id) as deals_closed,
    SUM(cd.deal_value) as revenue_generated,
    ROUND(SUM(cd.deal_value) / NULLIF(c.budget, 0), 2) as roi_ratio
FROM sales_analytics_demo.marketing_data.campaigns c
LEFT JOIN sales_analytics_demo.marketing_data.leads l ON c.campaign_id = l.campaign_id
LEFT JOIN sales_analytics_demo.marketing_data.marketing_attribution ma ON c.campaign_id = ma.campaign_id
LEFT JOIN sales_analytics_demo.sales_data.closed_deals cd ON ma.opportunity_id = cd.opportunity_id
WHERE cd.status = 'Won'
GROUP BY c.campaign_name, c.campaign_type, c.budget
HAVING revenue_generated > 0
ORDER BY roi_ratio DESC
LIMIT 15;
```

### Support Analysis

```sql
-- Top reasons for losing deals
SELECT 
    win_loss_reason,
    COUNT(*) as lost_deal_count,
    SUM(amount) as lost_revenue,
    ROUND(AVG(sales_cycle_days), 1) as avg_sales_cycle
FROM sales_analytics_demo.sales_data.closed_deals cd
JOIN sales_analytics_demo.sales_data.opportunities o ON cd.opportunity_id = o.opportunity_id
WHERE cd.status = 'Lost'
GROUP BY win_loss_reason
ORDER BY lost_deal_count DESC;

-- SLA compliance by customer tier
SELECT 
    c.customer_tier,
    COUNT(st.ticket_id) as total_tickets,
    COUNT(CASE WHEN st.sla_status = 'Met' THEN 1 END) as sla_met,
    COUNT(CASE WHEN st.sla_status = 'Breached' THEN 1 END) as sla_breached,
    ROUND(COUNT(CASE WHEN st.sla_status = 'Met' THEN 1 END) * 100.0 / COUNT(st.ticket_id), 2) as compliance_rate
FROM sales_analytics_demo.support_data.support_tickets st
JOIN sales_analytics_demo.sales_data.customers c ON st.customer_id = c.customer_id
GROUP BY c.customer_tier
ORDER BY compliance_rate;
```

---

## 🔧 Troubleshooting

### Common Issues

**Issue: "File not found" errors**
```bash
# Check S3 path
aws s3 ls s3://your-bucket/sales-demo-data/ --recursive

# Verify file naming matches code
# Files should be: sales_data/customers.csv, usage_data/product_usage_summary.csv, etc.
```

**Issue: Schema inference errors in Databricks**
```python
# Explicitly specify schema instead of inferSchema
from pyspark.sql.types import *

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    # ... add all fields
])

df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("s3a://bucket/path/customers.csv")
```

**Issue: Snowflake COPY command fails**
```sql
-- Check for errors in load history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'CUSTOMERS',
    START_TIME=>DATEADD(hours, -1, CURRENT_TIMESTAMP())
));

-- Use ON_ERROR = 'CONTINUE' to skip bad rows
COPY INTO table_name
FROM @stage
ON_ERROR = 'CONTINUE';
```

**Issue: Date format errors**
```python
# In Python generator, ensure consistent date format
df['created_date'] = pd.to_datetime(df['created_date']).dt.strftime('%Y-%m-%d')
```

---

## 📈 Next Steps

1. **Create Dashboards**
   - Snowflake: Use Snowsight or connect Tableau/Power BI
   - Databricks: Use Databricks SQL or Lakeview dashboards

2. **Set Up Incremental Loads** (Optional)
   - Modify generators to append new data
   - Use MERGE statements for upserts

3. **Optimize for Performance**
   ```sql
   -- Snowflake: Cluster keys
   ALTER TABLE closed_deals CLUSTER BY (quarter, region);
   
   -- Databricks: Z-ordering
   OPTIMIZE sales_data.closed_deals ZORDER BY (quarter, region);
   ```

4. **Add Security**
   ```sql
   -- Row-level security example
   CREATE ROW ACCESS POLICY region_policy
   AS (region STRING) RETURNS BOOLEAN ->
       CURRENT_ROLE() = 'ADMIN' OR region = CURRENT_REGION();
   ```

---

## 📚 Schema Documentation

### Table Relationships

```
customers (1) ----< (N) customer_products
    |                       |
    |                       +---< opportunities
    |                               |
    +---< support_tickets          +---< closed_deals
    |                               |
    +---< invoices                  +---< quotes
    |
    +---< customer_contacts
    |
    +---< customer_health_score

products (1) ----< (N) customer_products
    |
    +---< opportunities

campaigns (1) ----< (N) leads
    |                       |
    |                       +---< campaign_engagement
    +---< events
```

### Key Metrics Available

- **Sales**: Win rate, sales cycle, quota attainment, pipeline coverage
- **Usage**: License utilization, feature adoption, API usage, health scores
- **Marketing**: Campaign ROI, lead conversion, attribution, engagement
- **Support**: CSAT, SLA compliance, resolution time, ticket volume
- **Customer Success**: Health scores, churn risk, expansion opportunities

---

## 🤝 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review sample queries for common use cases
3. Validate data generation completed for all 62 tables
4. Ensure S3/storage paths are correct

## 📄 License

This synthetic dataset is provided for demo and educational purposes.

---

**Happy Analyzing! 🎉**
   