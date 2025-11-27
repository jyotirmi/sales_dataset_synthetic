# ⚡ Quick Start: Load into Fresh Snowflake Instance

## 🎯 TL;DR - 4 Simple Steps

```bash
# 1. Generate data (if not already done)
python3 scripts/sales_data_generator_part1.py
python3 scripts/sales_data_generator_part2.py
python3 scripts/sales_data_generator_part3.py
python3 scripts/sales_data_generator_part4.py

# 2. Verify data generation
python3 scripts/verify_dataset.py

# 3. Set up Snowflake (run DDL script in Snowflake Web UI)
#    Open: sql/snowflake_ddl_script.sql
#    Execute in Snowflake Web UI

# 4. Load data into Snowflake
#    Edit: scripts/load_to_snowflake.py (update credentials)
python3 scripts/load_to_snowflake.py
```

---

## 📝 Detailed Steps

### Step 1: Generate Data Files
```bash
cd /path/to/synthetic_data_sales  # Navigate to project directory
python3 scripts/sales_data_generator_part1.py
python3 scripts/sales_data_generator_part2.py
python3 scripts/sales_data_generator_part3.py
python3 scripts/sales_data_generator_part4.py
python3 scripts/verify_dataset.py  # Should show 62 tables
```

### Step 2: Create Snowflake Database & Tables
1. Log into [Snowflake Web UI](https://app.snowflake.com)
2. Open a new worksheet
3. Copy/paste contents of `sql/snowflake_ddl_script.sql`
4. Execute the script (creates database, schemas, and all 62 tables)

### Step 3: Configure Snowflake Credentials
Set environment variables before running the loader:
```bash
export SNOWFLAKE_USER="your_username@company.com"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"  # Your account identifier
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"  # Your warehouse name
```

Or create a `.env` file (copy from `.env.example`):
```bash
cp .env.example .env
# Edit .env with your credentials
```

**Note:** The script now uses environment variables instead of hardcoded credentials for security.
    'warehouse': 'COMPUTE_WH',  # Your warehouse name
    'database': 'sales_analytics_demo',
    'schema': 'PUBLIC'
}
```

### Step 4: Load Data
```bash
python3 load_to_snowflake.py
```

The script will:
- ✅ Connect to Snowflake
- ✅ Load all 62 CSV files
- ✅ Show progress for each table
- ✅ Display verification summary

---

## ✅ Expected Output

```
✅ Successfully loaded: 62 tables
📊 Total rows loaded: ~1,500,000

Verification:
  Customers          :    1,000 rows
  Products           :       18 rows
  Opportunities      :    4,000 rows
  Closed Deals       :    2,500 rows
  Support Tickets    :   10,000 rows
  Campaigns          :       75 rows
  Leads              :    7,000 rows
  Product Usage      :   20,000 rows
```

---

## 🔧 Troubleshooting

**"Please update SNOWFLAKE_CONFIG"**
→ Set environment variables (see Step 3)

**"Database not found"**
→ Run `sql/snowflake_ddl_script.sql` in Snowflake Web UI first

**"Table does not exist"**
→ Make sure DDL script completed successfully

**"No CSV files found"**
→ Run the generator scripts first (Step 1)

## 🔒 Optional: Row-Level Security (Advanced)

If you want to implement row-level security so different users see different data:

1. **After loading data**, run `sql/security_policies_snowflake.sql` in Snowflake Web UI
2. This creates:
   - User access mapping table
   - Row access policies for sales rep data filtering
   - Sample users and roles (sales reps, managers, admins)
3. **Use case**: Sales reps see only their opportunities, managers see their team's data

**Note**: This is optional and only needed if you want multi-user access control.

---

## 📚 Full Documentation

For detailed instructions, troubleshooting, and advanced options, see:
- **`SNOWFLAKE_LOADING_GUIDE.md`** - Complete step-by-step guide
- **`complete_readme.md`** - Full documentation with sample queries

---

**That's it! Your data should now be loaded into Snowflake. 🎉**

