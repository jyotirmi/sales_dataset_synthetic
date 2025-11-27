# 🚀 Complete Guide: Loading Sales Analytics Data into Fresh Snowflake Instance

This guide walks you through the complete process of loading the sales analytics dataset into a **fresh Snowflake instance** from scratch.

---

## 📋 Prerequisites

Before starting, ensure you have:

- ✅ Python 3.8+ installed
- ✅ Snowflake account with appropriate privileges
- ✅ Snowflake credentials (username, password, account identifier)
- ✅ Required Python packages: `pandas`, `numpy`, `snowflake-connector-python`
- ✅ (Optional) AWS S3 bucket for staging files

---

## 🔄 Step-by-Step Process

### **Step 1: Generate the Data Files**

If you haven't generated the data yet, or want to regenerate fresh data:

```bash
# Navigate to project directory
cd /path/to/synthetic_data_sales

# Activate virtual environment (if using one)
source venv/bin/activate  # On macOS/Linux
# OR
venv\Scripts\activate  # On Windows

# Install required packages (if not already installed)
pip install pandas numpy snowflake-connector-python

# Run all four generator scripts in order
python3 scripts/sales_data_generator_part1.py
python3 scripts/sales_data_generator_part2.py
python3 scripts/sales_data_generator_part3.py
python3 scripts/sales_data_generator_part4.py

# Verify all 62 tables were generated
python3 scripts/verify_dataset.py
```

**Expected Output:**
- Directory: `sales_analytics_data/` with subdirectories:
  - `sales_data/` (17 CSV files)
  - `usage_data/` (11 CSV files)
  - `marketing_data/` (13 CSV files)
  - `support_data/` (12 CSV files)
  - `operational_data/` (9 CSV files)

---

### **Step 2: Set Up Snowflake Database and Schemas**

#### Option A: Using Snowflake Web UI (Recommended for first-time setup)

1. **Log into Snowflake Web UI**
   - Go to https://app.snowflake.com
   - Log in with your credentials

2. **Open a New Worksheet**
   - Click "Worksheets" in the left sidebar
   - Click "+" to create a new worksheet

3. **Run the DDL Script**
   - Open `snowflake_ddl_script.sql` in your editor
   - Copy the entire contents
   - Paste into the Snowflake worksheet
   - **IMPORTANT**: Update the following before running:
     ```sql
     -- If using S3, uncomment and configure:
     -- CREATE OR REPLACE STAGE sales_analytics_demo.public.s3_stage
     --   URL = 's3://your-bucket-name/sales-demo-data/'
     --   CREDENTIALS = (AWS_KEY_ID = 'your_access_key' AWS_SECRET_KEY = 'your_secret_key')
     --   FILE_FORMAT = sales_analytics_demo.public.csv_format;
     ```
   - Execute the script (Ctrl+Enter or click "Run All")

4. **Verify Creation**
   ```sql
   -- Check database exists
   SHOW DATABASES LIKE 'SALES_ANALYTICS_DEMO';
   
   -- Check schemas
   USE DATABASE sales_analytics_demo;
   SHOW SCHEMAS;
   
   -- Check tables in each schema
   SHOW TABLES IN SCHEMA sales_data;
   SHOW TABLES IN SCHEMA usage_data;
   SHOW TABLES IN SCHEMA marketing_data;
   SHOW TABLES IN SCHEMA support_data;
   SHOW TABLES IN SCHEMA operational_data;
   ```

#### Option B: Using SnowSQL (Command Line)

```bash
# Install SnowSQL if not already installed
# Download from: https://developers.snowflake.com/snowsql/

# Connect and run DDL
snowsql -a YOUR_ACCOUNT -u YOUR_USERNAME -d sales_analytics_demo << EOF
-- Run the DDL script
$(cat sql/snowflake_ddl_script.sql)
EOF
```

---

### **Step 3: Load Data into Snowflake**

You have **three options** for loading data. Choose the one that fits your setup:

---

#### **Option 1: Python Script (Recommended - Easiest)**

Create and run a Python script to load all CSV files:

```python
# File: scripts/load_to_snowflake.py

import snowflake.connector
import pandas as pd
import glob
import os
from pathlib import Path

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================
SNOWFLAKE_CONFIG = {
    'user': 'YOUR_USERNAME',
    'password': 'YOUR_PASSWORD',
    'account': 'YOUR_ACCOUNT_IDENTIFIER',  # e.g., 'xy12345.us-east-1'
    'warehouse': 'COMPUTE_WH',  # or your warehouse name
    'database': 'sales_analytics_demo',
    'schema': 'PUBLIC'  # Default schema for staging
}

DATA_DIR = 'sales_analytics_data'

# ============================================================================
# Connect to Snowflake
# ============================================================================
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cursor = conn.cursor()

# ============================================================================
# Load all CSV files
# ============================================================================
print("\n" + "="*80)
print("Loading data into Snowflake...")
print("="*80)

# Map schema directory names to Snowflake schema names
schema_mapping = {
    'sales_data': 'sales_data',
    'usage_data': 'usage_data',
    'marketing_data': 'marketing_data',
    'support_data': 'support_data',
    'operational_data': 'operational_data'
}

total_tables = 0
total_rows = 0

# Get all CSV files
csv_files = glob.glob(f'{DATA_DIR}/**/*.csv', recursive=True)

for csv_file in sorted(csv_files):
    # Parse schema and table name from path
    path_parts = Path(csv_file).parts
    schema_dir = path_parts[-2]  # e.g., 'sales_data'
    table_name = Path(csv_file).stem  # e.g., 'customers'
    
    if schema_dir not in schema_mapping:
        print(f"⚠️  Skipping {csv_file} - unknown schema")
        continue
    
    snowflake_schema = schema_mapping[schema_dir]
    
    try:
        # Read CSV file
        print(f"\n📄 Loading {snowflake_schema}.{table_name}...", end=" ")
        df = pd.read_csv(csv_file)
        
        # Convert date columns (if any)
        date_columns = df.select_dtypes(include=['object']).columns
        for col in date_columns:
            if 'date' in col.lower() or 'created' in col.lower() or 'updated' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                except:
                    pass
        
        # Use write_pandas for efficient loading
        from snowflake.connector.pandas_tools import write_pandas
        
        # Write to Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database=SNOWFLAKE_CONFIG['database'],
            schema=snowflake_schema,
            auto_create_table=False,  # Tables should already exist from DDL
            overwrite=True  # Overwrite existing data
        )
        
        if success:
            print(f"✓ Loaded {nrows:,} rows")
            total_tables += 1
            total_rows += nrows
        else:
            print(f"✗ Failed to load")
            
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        continue

# ============================================================================
# Summary
# ============================================================================
print("\n" + "="*80)
print(f"✅ Loading Complete!")
print(f"   Tables loaded: {total_tables}")
print(f"   Total rows: {total_rows:,}")
print("="*80)

# ============================================================================
# Verification Queries
# ============================================================================
print("\n" + "="*80)
print("Running verification queries...")
print("="*80)

verification_queries = [
    ("Customers", "SELECT COUNT(*) FROM sales_analytics_demo.sales_data.customers"),
    ("Products", "SELECT COUNT(*) FROM sales_analytics_demo.sales_data.products"),
    ("Opportunities", "SELECT COUNT(*) FROM sales_analytics_demo.sales_data.opportunities"),
    ("Support Tickets", "SELECT COUNT(*) FROM sales_analytics_demo.support_data.support_tickets"),
    ("Campaigns", "SELECT COUNT(*) FROM sales_analytics_demo.marketing_data.campaigns"),
]

for name, query in verification_queries:
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        print(f"  {name}: {result[0]:,} rows")
    except Exception as e:
        print(f"  {name}: Error - {str(e)}")

# Close connection
cursor.close()
conn.close()
print("\n✅ Done!")
```

**Run the script:**
```bash
# Update the configuration in the script first!
python3 scripts/load_to_snowflake.py
```

---

#### **Option 2: Using Snowflake Web UI + Internal Stage**

1. **Upload files to Internal Stage**

   In Snowflake Web UI, use the **PUT** command:

   ```sql
   USE DATABASE sales_analytics_demo;
   USE WAREHOUSE COMPUTE_WH;
   
   -- Upload files by schema
   PUT file:///path/to/sales_analytics_data/sales_data/*.csv @csv_stage/sales_data/ AUTO_COMPRESS=FALSE;
   PUT file:///path/to/sales_analytics_data/usage_data/*.csv @csv_stage/usage_data/ AUTO_COMPRESS=FALSE;
   PUT file:///path/to/sales_analytics_data/marketing_data/*.csv @csv_stage/marketing_data/ AUTO_COMPRESS=FALSE;
   PUT file:///path/to/sales_analytics_data/support_data/*.csv @csv_stage/support_data/ AUTO_COMPRESS=FALSE;
   PUT file:///path/to/sales_analytics_data/operational_data/*.csv @csv_stage/operational_data/ AUTO_COMPRESS=FALSE;
   ```

   **Note**: The file path format depends on your OS:
   - **Windows**: `file://C:/Users/YourName/path/to/sales_analytics_data/...`
   - **macOS/Linux**: `file:///Users/YourName/path/to/sales_analytics_data/...`

2. **Load data using COPY commands**

   ```sql
   -- Load customers table
   COPY INTO sales_analytics_demo.sales_data.customers
   FROM @csv_stage/sales_data/customers.csv
   FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format')
   ON_ERROR = 'CONTINUE';
   
   -- Load products table
   COPY INTO sales_analytics_demo.sales_data.products
   FROM @csv_stage/sales_data/products.csv
   FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format')
   ON_ERROR = 'CONTINUE';
   
   -- Repeat for all 62 tables...
   ```

   **Tip**: You can generate all COPY commands with a script or use the Python approach above.

---

#### **Option 3: Using S3 External Stage (For Large Datasets)**

1. **Upload CSV files to S3**
   ```bash
   # Using AWS CLI
   aws s3 sync sales_analytics_data/ s3://your-bucket-name/sales-demo-data/
   
   # Verify upload
   aws s3 ls s3://your-bucket-name/sales-demo-data/ --recursive | wc -l
   # Should show 62 files
   ```

2. **Create S3 External Stage in Snowflake**
   ```sql
   USE DATABASE sales_analytics_demo;
   
   CREATE OR REPLACE STAGE sales_analytics_demo.public.s3_stage
     URL = 's3://your-bucket-name/sales-demo-data/'
     CREDENTIALS = (AWS_KEY_ID = 'your_access_key' AWS_SECRET_KEY = 'your_secret_key')
     FILE_FORMAT = sales_analytics_demo.public.csv_format;
   ```

3. **Load data using COPY commands**
   ```sql
   -- Load from S3 stage
   COPY INTO sales_analytics_demo.sales_data.customers
   FROM @s3_stage/sales_data/customers.csv
   FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format')
   ON_ERROR = 'CONTINUE';
   
   -- Repeat for all tables...
   ```

---

### **Step 4: Verify Data Load**

Run verification queries in Snowflake:

```sql
USE DATABASE sales_analytics_demo;

-- Check row counts for key tables
SELECT 'customers' as table_name, COUNT(*) as row_count 
FROM sales_data.customers
UNION ALL
SELECT 'products', COUNT(*) FROM sales_data.products
UNION ALL
SELECT 'opportunities', COUNT(*) FROM sales_data.opportunities
UNION ALL
SELECT 'closed_deals', COUNT(*) FROM sales_data.closed_deals
UNION ALL
SELECT 'support_tickets', COUNT(*) FROM support_data.support_tickets
UNION ALL
SELECT 'campaigns', COUNT(*) FROM marketing_data.campaigns
UNION ALL
SELECT 'leads', COUNT(*) FROM marketing_data.leads
ORDER BY table_name;

-- Check all schemas have tables
SELECT table_schema, COUNT(*) as table_count
FROM information_schema.tables
WHERE table_catalog = 'SALES_ANALYTICS_DEMO'
  AND table_schema IN ('SALES_DATA', 'USAGE_DATA', 'MARKETING_DATA', 'SUPPORT_DATA', 'OPERATIONAL_DATA')
GROUP BY table_schema
ORDER BY table_schema;

-- Sample data check
SELECT * FROM sales_data.customers LIMIT 5;
SELECT * FROM sales_data.products LIMIT 5;
```

**Expected Results:**
- `customers`: ~1,000 rows
- `products`: 18 rows
- `opportunities`: ~4,000 rows
- `closed_deals`: ~2,500 rows
- All 5 schemas should show tables

---

## 🔧 Troubleshooting

### **Issue: "Table does not exist" error**
**Solution**: Make sure you ran the DDL script first to create all tables.

### **Issue: "Invalid date format" errors**
**Solution**: The Python script handles date conversion automatically. If using COPY commands, ensure your CSV dates are in `YYYY-MM-DD` format.

### **Issue: "Warehouse not found" error**
**Solution**: 
```sql
-- Create a warehouse if needed
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

### **Issue: "Access denied" errors**
**Solution**: Ensure your Snowflake user has:
- `CREATE DATABASE` privilege (for initial setup)
- `CREATE SCHEMA` privilege
- `CREATE TABLE` privilege
- `INSERT` privilege on all tables
- `USAGE` privilege on warehouse

### **Issue: Slow loading**
**Solution**: 
- Use a larger warehouse size temporarily
- Load tables in parallel (if using Python, use threading)
- Consider using S3 external stage for large files

---

## 📊 Quick Test Queries

After loading, test with these queries:

```sql
-- Sales performance overview
SELECT 
    quarter,
    status,
    COUNT(*) as deal_count,
    SUM(deal_value) as total_revenue
FROM sales_data.closed_deals
WHERE fiscal_year = 2024
GROUP BY quarter, status
ORDER BY quarter;

-- Customer health summary
SELECT 
    customer_tier,
    COUNT(*) as customer_count,
    AVG(overall_score) as avg_health_score
FROM sales_data.customers c
JOIN operational_data.customer_health_score chs ON c.customer_id = chs.customer_id
GROUP BY customer_tier
ORDER BY customer_tier;

-- Support ticket volume by month
SELECT 
    DATE_TRUNC('month', created_date) as month,
    COUNT(*) as ticket_count,
    COUNT(CASE WHEN sla_status = 'Breached' THEN 1 END) as sla_breaches
FROM support_data.support_tickets
GROUP BY month
ORDER BY month;
```

---

## ✅ Success Checklist

- [ ] All 4 generator scripts completed successfully
- [ ] `verify_dataset.py` shows all 62 tables generated
- [ ] Snowflake database and schemas created
- [ ] All 62 tables created in Snowflake
- [ ] Data loaded into all tables
- [ ] Verification queries return expected row counts
- [ ] Sample queries run successfully

---

## 🎉 Next Steps

Once data is loaded:

1. **Create Views** for common queries
2. **Set up Dashboards** in Snowsight or connect BI tools
3. **Configure Row-Level Security** (Optional - Advanced)
   - See `sql/security_policies_snowflake.sql` for a complete row-level security implementation
   - This script creates policies so sales reps only see their own data, managers see their team's data
   - Run this script in Snowflake Web UI **after** data is loaded if you need RLS
4. **Optimize Tables** with clustering keys for better performance
5. **Create Sample Reports** using the sample queries in `complete_readme.md`

---

**Happy Analyzing! 🚀**

