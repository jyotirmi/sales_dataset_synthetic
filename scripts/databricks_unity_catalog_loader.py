# Databricks notebook source
# MAGIC %md
# MAGIC # Sales Analytics Demo - Unity Catalog Volume Loader
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC 1. Upload all CSV files to a Unity Catalog Volume
# MAGIC 2. Update configuration below with your catalog, schema, and volume names
# MAGIC 3. Run this notebook to create all 62 Delta tables
# MAGIC 
# MAGIC **Unity Catalog Volume Path Format:**
# MAGIC `/Volumes/<catalog>/<schema>/<volume>/<path>`
# MAGIC 
# MAGIC **Estimated Runtime:** 15-30 minutes for all 62 tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# ============================================================================
# UNITY CATALOG CONFIGURATION - MODIFY THESE VALUES
# ============================================================================

# Unity Catalog names
CATALOG_NAME = "sales_analytics"              # Your catalog name
SCHEMA_NAME = "raw_data"                      # Schema where volume exists
VOLUME_NAME = "csv_uploads"                   # Your volume name

# Full volume path - files should be here
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

# Target schemas for Delta tables
TARGET_CATALOG = "sales_analytics"            # Where to create tables
TARGET_SCHEMAS = {
    'sales': 'sales_data',
    'usage': 'usage_data',
    'marketing': 'marketing_data',
    'support': 'support_data',
    'operational': 'operational_data'
}

# File organization in volume
FILES_IN_FOLDERS = True  # True if: /Volumes/.../sales_data/customers.csv
                         # False if: /Volumes/.../customers.csv

# ============================================================================

print(f"📁 Volume Path: {VOLUME_PATH}")
print(f"📊 Target Catalog: {TARGET_CATALOG}")
print(f"📂 Files in folders: {FILES_IN_FOLDERS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify Volume Access and List Files

# COMMAND ----------

# Check if volume path exists and is accessible
try:
    files = dbutils.fs.ls(VOLUME_PATH)
    print(f"✓ Successfully accessed volume at: {VOLUME_PATH}")
    print(f"✓ Found {len(files)} items\n")
    
    # Show first 20 items
    print("First 20 items in volume:")
    for file in files[:20]:
        size_mb = file.size / (1024 * 1024)
        item_type = "📁 DIR " if file.isDir() else "📄 FILE"
        print(f"  {item_type} {file.name:<50} {size_mb:>10.2f} MB")
    
    if len(files) > 20:
        print(f"\n  ... and {len(files) - 20} more items")
        
except Exception as e:
    print(f"❌ Error accessing volume: {str(e)}")
    print("\n🔍 Troubleshooting steps:")
    print(f"1. Verify catalog exists: SHOW CATALOGS")
    print(f"2. Verify schema exists: SHOW SCHEMAS IN {CATALOG_NAME}")
    print(f"3. Verify volume exists: SHOW VOLUMES IN {CATALOG_NAME}.{SCHEMA_NAME}")
    print(f"4. Check permissions: You need USE CATALOG, USE SCHEMA, and READ VOLUME")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Target Catalog and Schemas

# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"USE CATALOG {TARGET_CATALOG}")

print(f"✓ Using catalog: {TARGET_CATALOG}\n")

# Create all target schemas
for schema_key, schema_name in TARGET_SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{schema_name}")
    print(f"✓ Created schema: {TARGET_CATALOG}.{schema_name}")

print(f"\n✅ All schemas ready in Unity Catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Define All Tables Configuration

# COMMAND ----------

# Complete list of all 62 tables organized by schema
tables_config = {
    'sales': [
        'customers', 'products', 'customer_products', 'opportunities',
        'closed_deals', 'sales_targets', 'customer_attributes',
        'customer_contacts', 'account_hierarchy', 'customer_notes',
        'competitive_intelligence', 'quotes', 'quote_line_items',
        'contracts', 'revenue_recognition', 'territories'
    ],
    'usage': [
        'product_usage_summary', 'feature_usage', 'telemetry_events',
        'product_health_alerts', 'user_sessions', 'api_usage',
        'integration_usage', 'license_usage', 'data_quality_metrics',
        'performance_metrics'
    ],
    'marketing': [
        'campaigns', 'events', 'leads', 'campaign_engagement',
        'marketing_attribution', 'webinar_attendance', 'content_assets',
        'email_campaigns', 'social_media_engagement', 'website_traffic',
        'partner_referrals', 'marketing_budget', 'lead_scoring_rules'
    ],
    'support': [
        'support_tickets', 'ticket_comments', 'ticket_resolution',
        'customer_satisfaction', 'sla_policies', 'sla_breaches',
        'knowledge_base', 'ticket_tags', 'escalations',
        'recurring_issues', 'support_agent_metrics', 'support_channels',
        'ticket_worklog'
    ],
    'operational': [
        'customer_health_score', 'onboarding_progress', 'product_releases',
        'customer_success_activities', 'user_accounts', 'audit_log',
        'notifications', 'invoices', 'payments', 'renewals'
    ]
}

# Count total tables
total_tables = sum(len(tables) for tables in tables_config.values())
print(f"📊 Will load {total_tables} tables across 5 schemas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Helper Function to Load Tables from Volume

# COMMAND ----------

from datetime import datetime
import time

def load_table_from_volume(schema_key, table_name, volume_path, target_catalog, target_schemas, files_in_folders):
    """
    Load a CSV file from Unity Catalog Volume into a Delta table
    
    Args:
        schema_key: Schema key (sales, usage, marketing, support, operational)
        table_name: Name of the table
        volume_path: Base volume path
        target_catalog: Target catalog name
        target_schemas: Dict mapping schema keys to schema names
        files_in_folders: Whether files are organized in folders
    
    Returns:
        Tuple of (success: bool, row_count: int, message: str)
    """
    start_time = time.time()
    
    try:
        # Construct file path in volume
        if files_in_folders:
            # Format: /Volumes/catalog/schema/volume/sales_data/customers.csv
            file_path = f"{volume_path}/{schema_key}_data/{table_name}.csv"
        else:
            # Format: /Volumes/catalog/schema/volume/customers.csv
            file_path = f"{volume_path}/{table_name}.csv"
        
        # Check if file exists
        try:
            dbutils.fs.ls(file_path)
        except:
            return False, 0, f"File not found: {file_path}"
        
        # Read CSV from volume with comprehensive options
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("nullValue", "NULL") \
            .option("emptyValue", "") \
            .option("dateFormat", "yyyy-MM-dd") \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .load(file_path)
        
        # Get row count
        row_count = df.count()
        
        if row_count == 0:
            return False, 0, "File is empty"
        
        # Construct full table name in Unity Catalog
        target_schema = target_schemas[schema_key]
        full_table_name = f"{target_catalog}.{target_schema}.{table_name}"
        
        # Write to Delta table in Unity Catalog
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true") \
            .saveAsTable(full_table_name)
        
        # Calculate duration
        duration = time.time() - start_time
        
        return True, row_count, f"Loaded in {duration:.2f}s"
        
    except Exception as e:
        return False, 0, f"Error: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Load All Tables with Progress Tracking

# COMMAND ----------

# Initialize tracking
results = []
successful_loads = 0
failed_loads = 0
total_rows = 0
overall_start = time.time()

print("="*80)
print("STARTING DATA LOAD FROM UNITY CATALOG VOLUME - ALL 62 TABLES")
print("="*80)

# Load each schema
for schema_key, table_list in tables_config.items():
    schema_name = TARGET_SCHEMAS[schema_key]
    print(f"\n{'='*80}")
    print(f"📁 Loading {schema_name.upper()} Schema ({len(table_list)} tables)")
    print(f"{'='*80}\n")
    
    schema_start = time.time()
    schema_rows = 0
    schema_success = 0
    
    for table in table_list:
        # Load the table
        success, row_count, message = load_table_from_volume(
            schema_key, 
            table, 
            VOLUME_PATH,
            TARGET_CATALOG,
            TARGET_SCHEMAS,
            FILES_IN_FOLDERS
        )
        
        # Track results
        results.append({
            'schema': schema_key,
            'table': table,
            'success': success,
            'row_count': row_count,
            'message': message
        })
        
        if success:
            successful_loads += 1
            total_rows += row_count
            schema_rows += row_count
            schema_success += 1
            print(f"  ✓ {table:<35} {row_count:>10,} rows - {message}")
        else:
            failed_loads += 1
            print(f"  ✗ {table:<35} {'FAILED':>10} - {message}")
    
    schema_duration = time.time() - schema_start
    print(f"\n  Schema Summary: {schema_success}/{len(table_list)} tables loaded, {schema_rows:,} total rows in {schema_duration:.1f}s")

# Calculate total duration
total_duration = time.time() - overall_start

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Load Summary Report

# COMMAND ----------

print("\n" + "="*80)
print("LOAD SUMMARY REPORT")
print("="*80)

print(f"\n📊 Overall Statistics:")
print(f"  Total tables expected: {total_tables}")
print(f"  Successfully loaded: {successful_loads}")
print(f"  Failed: {failed_loads}")
print(f"  Total rows loaded: {total_rows:,}")
print(f"  Total duration: {total_duration/60:.2f} minutes")
print(f"  Average speed: {total_rows/(total_duration) if total_duration > 0 else 0:,.0f} rows/second")

# Failed tables report
if failed_loads > 0:
    print(f"\n❌ Failed Tables ({failed_loads}):")
    for result in results:
        if not result['success']:
            print(f"  - {result['schema']}/{result['table']}: {result['message']}")
else:
    print(f"\n✅ All tables loaded successfully!")

# Schema breakdown
print(f"\n📁 Rows by Schema:")
for schema_key, schema_name in TARGET_SCHEMAS.items():
    schema_rows = sum(r['row_count'] for r in results if r['schema'] == schema_key and r['success'])
    schema_tables = sum(1 for r in results if r['schema'] == schema_key and r['success'])
    total_schema_tables = len(tables_config[schema_key])
    print(f"  {schema_name}: {schema_rows:>12,} rows across {schema_tables}/{total_schema_tables} tables")

print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Data Validation Queries

# COMMAND ----------

print("\n" + "="*80)
print("DATA VALIDATION")
print("="*80 + "\n")

# Validation 1: Check key table counts
print("✓ Key Table Record Counts:")
validation_queries = [
    (f"SELECT COUNT(*) as count FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}.customers", "Customers"),
    (f"SELECT COUNT(*) as count FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}.opportunities", "Opportunities"),
    (f"SELECT COUNT(*) as count FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}.closed_deals", "Closed Deals"),
    (f"SELECT COUNT(*) as count FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['marketing']}.leads", "Leads"),
    (f"SELECT COUNT(*) as count FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['support']}.support_tickets", "Support Tickets"),
]

for query, label in validation_queries:
    try:
        count = spark.sql(query).collect()[0]['count']
        print(f"  {label:<25} {count:>10,}")
    except Exception as e:
        print(f"  {label:<25} {'ERROR':>10} - {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Sample Data Preview

# COMMAND ----------

print("\n" + "="*80)
print("SAMPLE DATA PREVIEW")
print("="*80 + "\n")

# Preview customers
print("📋 Customers (first 5 rows):")
display(spark.sql(f"SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}.customers LIMIT 5"))

# Preview opportunities
print("\n📋 Opportunities (first 5 rows):")
display(spark.sql(f"SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}.opportunities LIMIT 5"))

# Preview support tickets
print("\n📋 Support Tickets (first 5 rows):")
display(spark.sql(f"SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['support']}.support_tickets LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create Sample Views in Unity Catalog

# COMMAND ----------

# Create useful analytical views

sales_schema = f"{TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}"
support_schema = f"{TARGET_CATALOG}.{TARGET_SCHEMAS['support']}"

# Customer 360 View
spark.sql(f"""
CREATE OR REPLACE VIEW {sales_schema}.customer_360 AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.industry,
    c.company_size,
    c.region,
    c.customer_tier,
    COUNT(DISTINCT cp.product_id) as product_count,
    SUM(cp.contract_value) as total_contract_value,
    COUNT(DISTINCT o.opportunity_id) as opportunity_count
FROM {sales_schema}.customers c
LEFT JOIN {sales_schema}.customer_products cp ON c.customer_id = cp.customer_id
LEFT JOIN {sales_schema}.opportunities o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.industry, c.company_size, c.region, c.customer_tier
""")

print("✓ Created view: customer_360")

# Sales Performance View
spark.sql(f"""
CREATE OR REPLACE VIEW {sales_schema}.sales_performance AS
SELECT 
    quarter,
    fiscal_year,
    region,
    status,
    COUNT(*) as deal_count,
    SUM(deal_value) as total_revenue,
    AVG(deal_value) as avg_deal_size,
    AVG(sales_cycle_days) as avg_sales_cycle
FROM {sales_schema}.closed_deals
GROUP BY quarter, fiscal_year, region, status
""")

print("✓ Created view: sales_performance")

# Support Metrics View
spark.sql(f"""
CREATE OR REPLACE VIEW {support_schema}.support_metrics AS
SELECT 
    DATE_TRUNC('month', created_date) as month,
    COUNT(*) as total_tickets,
    COUNT(CASE WHEN status IN ('Resolved', 'Closed') THEN 1 END) as resolved_tickets,
    COUNT(CASE WHEN sla_status = 'Met' THEN 1 END) as sla_met,
    COUNT(CASE WHEN sla_status = 'Breached' THEN 1 END) as sla_breached
FROM {support_schema}.support_tickets
GROUP BY DATE_TRUNC('month', created_date)
ORDER BY month DESC
""")

print("✓ Created view: support_metrics")

print("\n✅ Sample views created successfully in Unity Catalog!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Optimization (Optional but Recommended)

# COMMAND ----------

print("\n" + "="*80)
print("TABLE OPTIMIZATION")
print("="*80 + "\n")

print("🔧 Optimizing key tables for query performance...")
print("(This may take several minutes)\n")

# Optimize main tables with Z-ordering
optimization_configs = [
    (f"{TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}.customers", ["region", "industry"]),
    (f"{TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}.opportunities", ["stage", "close_date"]),
    (f"{TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}.closed_deals", ["quarter", "status", "region"]),
    (f"{TARGET_CATALOG}.{TARGET_SCHEMAS['marketing']}.leads", ["lead_status", "created_date"]),
    (f"{TARGET_CATALOG}.{TARGET_SCHEMAS['support']}.support_tickets", ["status", "priority", "created_date"]),
]

for table, zorder_cols in optimization_configs:
    try:
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({', '.join(zorder_cols)})")
        print(f"  ✓ Optimized {table}")
    except Exception as e:
        print(f"  ⚠ Could not optimize {table}: {str(e)}")

print("\n✅ Optimization complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps

# COMMAND ----------

print("\n" + "="*80)
print("🎉 DATA LOAD FROM UNITY CATALOG VOLUME COMPLETE!")
print("="*80)

if failed_loads == 0:
    print(f"""
✅ SUCCESS! All {total_tables} tables loaded successfully
📊 Total records: {total_rows:,}
⏱️  Total time: {total_duration/60:.2f} minutes
📁 Volume source: {VOLUME_PATH}
🗂️  Target catalog: {TARGET_CATALOG}

🔍 Explore Your Data in Unity Catalog:
  • Use Catalog Explorer in the left sidebar
  • Query tables using SQL or Python
  • Create dashboards in Databricks SQL

📊 Sample Queries:
  • SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}.customer_360 LIMIT 10
  • SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['sales']}.sales_performance ORDER BY fiscal_year DESC
  • SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMAS['support']}.support_metrics ORDER BY month DESC

🚀 Next Steps:
  1. Grant appropriate permissions: GRANT SELECT ON CATALOG {TARGET_CATALOG} TO ...
  2. Create Databricks SQL queries and dashboards
  3. Set up data lineage tracking
  4. Document your tables in Unity Catalog
  5. Consider setting up incremental loads for production use
""")
else:
    print(f"""
⚠️  PARTIAL SUCCESS: {successful_loads}/{total_tables} tables loaded
❌ Failed: {failed_loads} tables
📊 Total records loaded: {total_rows:,}

Please review the failed tables above and:
  1. Verify the CSV files were uploaded to the volume correctly
  2. Check file paths match the configuration
  3. Review error messages for specific issues
  4. Verify permissions on the Unity Catalog volume
  5. Re-run this notebook after fixing issues
""")

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Commands

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Unity Catalog Structure
# MAGIC ```sql
# MAGIC -- List catalogs
# MAGIC SHOW CATALOGS;
# MAGIC 
# MAGIC -- List schemas in your catalog
# MAGIC SHOW SCHEMAS IN sales_analytics;
# MAGIC 
# MAGIC -- List volumes in a schema
# MAGIC SHOW VOLUMES IN sales_analytics.raw_data;
# MAGIC 
# MAGIC -- List tables in a schema
# MAGIC SHOW TABLES IN sales_analytics.sales_data;
# MAGIC ```
# MAGIC 
# MAGIC ### Check Volume Contents
# MAGIC ```python
# MAGIC # List all files in volume
# MAGIC display(dbutils.fs.ls("/Volumes/sales_analytics/raw_data/csv_uploads/"))
# MAGIC 
# MAGIC # Check specific folder
# MAGIC display(dbutils.fs.ls("/Volumes/sales_analytics/raw_data/csv_uploads/sales_data/"))
# MAGIC 
# MAGIC # Preview a specific file
# MAGIC df = spark.read.csv("/Volumes/sales_analytics/raw_data/csv_uploads/sales_data/customers.csv", header=True)
# MAGIC display(df.limit(5))
# MAGIC ```
# MAGIC 
# MAGIC ### Check Table Details
# MAGIC ```sql
# MAGIC -- Describe table
# MAGIC DESCRIBE TABLE EXTENDED sales_analytics.sales_data.customers;
# MAGIC 
# MAGIC -- Show table history
# MAGIC DESCRIBE HISTORY sales_analytics.sales_data.customers;
# MAGIC 
# MAGIC -- Check table properties
# MAGIC SHOW TBLPROPERTIES sales_analytics.sales_data.customers;
# MAGIC ```
# MAGIC 
# MAGIC ### Grant Permissions
# MAGIC ```sql
# MAGIC -- Grant catalog access
# MAGIC GRANT USE CATALOG ON CATALOG sales_analytics TO `user@example.com`;
# MAGIC 
# MAGIC -- Grant schema access
# MAGIC GRANT USE SCHEMA ON SCHEMA sales_analytics.sales_data TO `user@example.com`;
# MAGIC 
# MAGIC -- Grant table access
# MAGIC GRANT SELECT ON TABLE sales_analytics.sales_data.customers TO `user@example.com`;
# MAGIC 
# MAGIC -- Grant all tables in schema
# MAGIC GRANT SELECT ON SCHEMA sales_analytics.sales_data TO `user@example.com`;
# MAGIC ```