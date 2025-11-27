# Databricks Upload Guide - Manual Process

## 📋 Overview

This guide covers uploading your 62 CSV files to Databricks and loading them into Delta tables **without using S3**.

## 🚀 Quick Start (3 Methods)

Choose the method that works best for you:

| Method | Best For | Difficulty | Time |
|--------|----------|------------|------|
| **Method 1: Databricks CLI** | Bulk upload, automation | Medium | 5-10 min |
| **Method 2: Databricks UI** | Small datasets, testing | Easy | 20-30 min |
| **Method 3: Python API** | Integration, scripting | Advanced | 10-15 min |

---

## Method 1: Databricks CLI (Recommended)

### Prerequisites

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token
```

When prompted, enter:
- **Databricks Host**: `https://<your-workspace>.cloud.databricks.com`
- **Token**: Generate from Databricks → User Settings → Access Tokens

### Step 1: Generate Data

```bash
python sales_data_generator_part1.py
python sales_data_generator_part2.py
python sales_data_generator_part3.py
python sales_data_generator_part4.py
python verify_dataset.py
```

### Step 2: Upload Files

**Linux/macOS:**
```bash
chmod +x databricks_cli_upload.sh
./databricks_cli_upload.sh
```

**Windows PowerShell:**
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\databricks_powershell_upload.ps1
```

**Manual Upload:**
```bash
# Navigate to your data directory
cd sales_analytics_data

# Upload each schema folder
databricks fs cp sales_data/ dbfs:/FileStore/tables/sales_data/ --recursive --overwrite
databricks fs cp usage_data/ dbfs:/FileStore/tables/usage_data/ --recursive --overwrite
databricks fs cp marketing_data/ dbfs:/FileStore/tables/marketing_data/ --recursive --overwrite
databricks fs cp support_data/ dbfs:/FileStore/tables/support_data/ --recursive --overwrite
databricks fs cp operational_data/ dbfs:/FileStore/tables/operational_data/ --recursive --overwrite

# Verify upload
databricks fs ls dbfs:/FileStore/tables/
```

### Step 3: Load Data in Databricks

1. Open your Databricks workspace
2. Go to **Workspace** → **Create** → **Notebook**
3. Copy/paste the **"Databricks Manual Upload"** notebook content
4. Update configuration:
   ```python
   DBFS_BASE_PATH = "/FileStore/tables/"
   FILES_IN_FOLDERS = True
   ```
5. Run all cells

---

## Method 2: Databricks UI Upload

### Step 1: Generate Data
(Same as Method 1)

### Step 2: Upload via UI

1. **Open Databricks Workspace**
2. Go to **Data** → **Create Table**
3. Click **Upload File**
4. Select CSV files (you can select multiple)
5. Files will upload to `/FileStore/tables/`

**Tips:**
- Upload in batches (10-20 files at a time)
- Keep browser tab active during upload
- Large files (>100MB) may timeout - use CLI instead

### Step 3: Load Data

Use the **"Databricks Manual Upload"** notebook with:
```python
DBFS_BASE_PATH = "/FileStore/tables/"
FILES_IN_FOLDERS = False  # Set to False if flat upload
```

---

## Method 3: Python API (Advanced)

### Prerequisites

```bash
pip install databricks-cli
```

### Upload Script

```python
from databricks_cli.sdk import ApiClient
from databricks_cli.dbfs.api import DbfsApi
from pathlib import Path

# Configuration
host = "https://<your-workspace>.cloud.databricks.com"
token = "<your-token>"

# Initialize API client
api_client = ApiClient(host=host, token=token)
dbfs_api = DbfsApi(api_client)

# Upload files
data_dir = Path("sales_analytics_data")
dbfs_target = "/FileStore/tables/"

for schema_dir in data_dir.iterdir():
    if schema_dir.is_dir():
        print(f"Uploading {schema_dir.name}...")
        
        # Create directory in DBFS
        dbfs_api.mkdirs(f"{dbfs_target}{schema_dir.name}")
        
        for csv_file in schema_dir.glob("*.csv"):
            print(f"  Uploading {csv_file.name}...")
            
            # Upload file
            dbfs_path = f"{dbfs_target}{schema_dir.name}/{csv_file.name}"
            dbfs_api.put_file(str(csv_file), dbfs_path, overwrite=True)

print("✅ Upload complete!")
```

---

## 📊 The Databricks Notebook

The **"Databricks Manual Upload"** notebook includes:

✅ **Automatic file discovery** - Finds your uploaded CSVs  
✅ **Progress tracking** - Shows upload status for all 62 tables  
✅ **Error handling** - Reports any failed loads  
✅ **Data validation** - Verifies row counts  
✅ **Sample queries** - Validates data loaded correctly  
✅ **View creation** - Creates useful analytical views  
✅ **Optimization** - Z-orders key tables for performance  

### Notebook Configuration

```python
# At the top of the notebook, configure:

DBFS_BASE_PATH = "/FileStore/tables/"  # Where you uploaded CSVs

# If you uploaded with folder structure:
# /FileStore/tables/sales_data/customers.csv
FILES_IN_FOLDERS = True

# If you uploaded flat:
# /FileStore/tables/customers.csv
FILES_IN_FOLDERS = False

# Unity Catalog (optional)
USE_UNITY_CATALOG = False  # Set True if using Unity Catalog
CATALOG_NAME = "sales_analytics"
```

---

## 🔍 Verification Steps

### 1. Verify Files Uploaded to DBFS

```bash
# List all uploaded files
databricks fs ls dbfs:/FileStore/tables/

# Check specific folder
databricks fs ls dbfs:/FileStore/tables/sales_data/

# Check file size
databricks fs ls -l dbfs:/FileStore/tables/sales_data/customers.csv
```

### 2. In Databricks Notebook

```python
# List files
display(dbutils.fs.ls("/FileStore/tables/"))

# Check specific file
df = spark.read.csv("/FileStore/tables/sales_data/customers.csv", header=True)
print(f"Rows: {df.count()}")
display(df.limit(5))
```

### 3. Verify Tables Created

```sql
-- List all databases
SHOW DATABASES;

-- List tables in a schema
SHOW TABLES IN sales_analytics_demo_sales;

-- Check row count
SELECT COUNT(*) FROM sales_analytics_demo_sales.customers;

-- Preview data
SELECT * FROM sales_analytics_demo_sales.customers LIMIT 5;
```

---

## 🐛 Troubleshooting

### Issue: "databricks: command not found"

**Solution:**
```bash
pip install databricks-cli
# Or
pip3 install databricks-cli
```

### Issue: "Error: Could not find token"

**Solution:**
```bash
databricks configure --token
# Enter your workspace URL and token
```

### Issue: "File not found" in notebook

**Solution:**
```python
# Check what's actually in DBFS
display(dbutils.fs.ls("/FileStore/tables/"))

# Update DBFS_BASE_PATH to match where files are
DBFS_BASE_PATH = "/FileStore/tables/"  # or your actual path
```

### Issue: Upload times out in UI

**Solution:** Use CLI method for large files
```bash
databricks fs cp large_file.csv dbfs:/FileStore/tables/ --overwrite
```

### Issue: "Schema mismatch" error

**Solution:**
```python
# In notebook, use overwriteSchema option
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("table_name")
```

### Issue: Permission denied

**Solution:**
- Check your Databricks token has appropriate permissions
- Ensure you have "Can Manage" or "Can Edit" on the workspace
- Contact your Databricks admin

### Issue: Out of memory

**Solution:**
```python
# Use a larger cluster or read in batches
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .load("/path/to/files/*.csv")
```

---

## 📁 Expected File Structure

After upload, your DBFS should look like:

```
/FileStore/tables/
├── sales_data/
│   ├── customers.csv
│   ├── products.csv
│   ├── opportunities.csv
│   └── ... (17 files total)
├── usage_data/
│   ├── product_usage_summary.csv
│   ├── api_usage.csv
│   └── ... (11 files total)
├── marketing_data/
│   ├── campaigns.csv
│   ├── leads.csv
│   └── ... (13 files total)
├── support_data/
│   ├── support_tickets.csv
│   ├── customer_satisfaction.csv
│   └── ... (13 files total)
└── operational_data/
    ├── customer_health_score.csv
    ├── invoices.csv
    └── ... (10 files total)
```

**Total: 62 CSV files**

---

## ⚡ Performance Tips

### 1. Upload Performance
- **Use CLI for files >50MB**
- Upload multiple files in parallel (separate terminal windows)
- Compress large CSVs before upload (optional)

### 2. Loading Performance
- Use a cluster with adequate resources (8+ cores recommended)
- Enable auto-scaling for variable workloads
- Use Delta Lake for better query performance

### 3. Cluster Configuration
```
Cluster Type: Standard
Workers: 2-4 (or auto-scale 2-8)
Instance Type: Standard_DS3_v2 or better
Databricks Runtime: 13.3 LTS or later
```

---

## 🎯 Success Checklist

- [ ] Databricks CLI installed and configured
- [ ] All 4 Python generator scripts run successfully
- [ ] verify_dataset.py confirms 62 tables present
- [ ] CSV files uploaded to DBFS
- [ ] Databricks notebook imported to workspace
- [ ] DBFS_BASE_PATH configured correctly in notebook
- [ ] All cells run without errors
- [ ] Validation queries return expected counts
- [ ] Sample views created successfully
- [ ] Tables optimized with Z-ordering

---

## 📞 Need Help?

**Check logs:**
```python
# In Databricks notebook
# Check failed loads from results list
failed = [r for r in results if not r['success']]
for f in failed:
    print(f"{f['schema']}/{f['table']}: {f['message']}")
```

**Common Commands:**
```bash
# Check Databricks version
databricks --version

# Test connection
databricks workspace ls /

# List DBFS files
databricks fs ls dbfs:/FileStore/tables/

# Download a file for inspection
databricks fs cp dbfs:/FileStore/tables/customers.csv ./test.csv

# Delete files (if needed to re-upload)
databricks fs rm -r dbfs:/FileStore/tables/sales_data/
```

---

## ✅ Expected Results

After successful completion:
- **62 Delta tables** created in Databricks
- **~1.5 million records** loaded
- **3-5 sample views** created
- **Tables optimized** for query performance
- **Ready for analytics** and dashboard creation

Load time: **15-30 minutes** depending on cluster size

---

## 🚀 Next Steps

1. **Create SQL Queries** - Use Databricks SQL Editor
2. **Build Dashboards** - Databricks SQL Dashboards or Lakeview
3. **Set up Jobs** - Schedule incremental loads if needed
4. **Grant Access** - Share with your team
5. **Connect BI Tools** - Tableau, Power BI, etc.

---

**Congratulations! Your sales analytics demo dataset is now loaded in Databricks! 🎉**