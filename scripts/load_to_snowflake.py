#!/usr/bin/env python3
"""
Sales Analytics Demo Dataset - Snowflake Loader
Loads all 62 CSV files into a fresh Snowflake instance

Usage:
    1. Update SNOWFLAKE_CONFIG below with your credentials
    2. Ensure data files are in sales_analytics_data/ directory
    3. Run: python3 load_to_snowflake.py
"""

import snowflake.connector
import glob
import os
from pathlib import Path

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Load .env file from project root
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
    env_path = os.path.join(PROJECT_ROOT, '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print("✓ Loaded environment variables from .env file")
except ImportError:
    # python-dotenv not installed, will use system environment variables only
    pass

# Verify pandas is available before importing write_pandas
try:
    import pandas as pd
    pd_version = pd.__version__
    from snowflake.connector.pandas_tools import write_pandas
except ImportError as e:
    print(f"❌ Error importing required packages: {e}")
    print("   Please install: pip install pandas snowflake-connector-python")
    raise

# ============================================================================
# CONFIGURATION - Use Environment Variables
# ============================================================================
# Set these environment variables before running:
#   export SNOWFLAKE_USER="your_username"
#   export SNOWFLAKE_PASSWORD="your_password"
#   export SNOWFLAKE_ACCOUNT="your_account"
#   export SNOWFLAKE_WAREHOUSE="your_warehouse"
#   export SNOWFLAKE_DATABASE="sales_analytics_demo"  # Optional, defaults shown
#   export SNOWFLAKE_SCHEMA="PUBLIC"  # Optional, defaults shown

SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER', 'YOUR_USERNAME'),
    'password': os.getenv('SNOWFLAKE_PASSWORD', 'YOUR_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT', 'YOUR_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'sales_analytics_demo'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
}

# Get project root directory (parent of scripts/)
# Note: SCRIPT_DIR and PROJECT_ROOT may already be set above if dotenv was loaded
if 'SCRIPT_DIR' not in locals():
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'sales_analytics_data')

# ============================================================================
# Helper Functions
# ============================================================================

def clean_value(val):
    """Convert pandas NA types to None - critical for Snowflake compatibility"""
    if val is None:
        return None
    
    # Check for string "NAN" or "nan" (pandas sometimes converts NaN to string)
    if isinstance(val, str) and val.upper() in ['NAN', 'NAT', '<NAT>', 'NONE', 'NULL']:
        return None
    
    # Check for pandas NA types
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    
    # Check for NaT specifically (datetime NaT)
    try:
        if hasattr(pd, 'NaT') and (val is pd.NaT or (isinstance(val, type(pd.NaT)) and pd.isna(val))):
            return None
    except:
        pass
    
    # Check for pd.NA
    try:
        if hasattr(pd, 'NA') and val is pd.NA:
            return None
    except:
        pass
    
    # Check for float NaN
    try:
        if isinstance(val, float) and (val != val):  # NaN check: NaN != NaN is True
            return None
    except:
        pass
    
    return val

def convert_date_columns(df):
    """Convert date-like columns to proper date format"""
    date_columns = df.select_dtypes(include=['object']).columns
    for col in date_columns:
        if any(keyword in col.lower() for keyword in ['date', 'created', 'updated', 'start', 'end', 'close']):
            try:
                # Convert to datetime first, then to date, handling NaT properly
                df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
                # Convert to date, but keep NaT as None
                df[col] = df[col].apply(lambda x: x.date() if pd.notna(x) else None)
            except Exception as e:
                # If conversion fails, leave as is
                pass
    return df

def convert_json_columns(df):
    """Convert JSON string columns to Python dicts for Snowflake VARIANT type"""
    import json
    json_columns = [col for col in df.columns if 'json' in col.lower()]
    
    for col in json_columns:
        try:
            # Convert JSON strings to Python dicts/objects
            # Snowflake VARIANT expects dict/list, not string
            def parse_json(val):
                if val is None or pd.isna(val):
                    return None
                if isinstance(val, (dict, list)):
                    return val  # Already parsed
                if isinstance(val, str):
                    try:
                        return json.loads(val) if val.strip() else None
                    except (json.JSONDecodeError, ValueError):
                        # If not valid JSON, return as string (will be converted to string in VARIANT)
                        return val
                return val
            
            df[col] = df[col].apply(parse_json)
        except Exception as e:
            # If conversion fails, leave as is
                pass
    return df

def load_table(conn, csv_file, schema_name, table_name, database_name):
    """Load a single CSV file into Snowflake table"""
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Remove internal columns that aren't in the database schema
        # usage_tier is an internal field for data generation, not in the actual schema
        if 'usage_tier' in df.columns:
            df = df.drop(columns=['usage_tier'])
        
        # Convert date columns
        df = convert_date_columns(df)
        
        # Convert JSON columns (for VARIANT type in Snowflake)
        df = convert_json_columns(df)
        
        # Replace all pandas NA/NaN/NaT values with None (Python None, not pandas NaT)
        # This is critical for Snowflake compatibility
        df = df.replace([pd.NA, pd.NaT], None)
        df = df.where(pd.notnull(df), None)
        
        # Validate and fix data issues before loading
        # 1. Fix ROI_PERCENT if it exceeds NUMBER(5,2) range (max 999.99)
        if 'roi_percent' in df.columns:
            def cap_roi(val):
                if pd.isna(val) or val is None:
                    return 0
                try:
                    val_float = float(val)
                    return min(val_float, 999.99)
                except (ValueError, TypeError):
                    return 0
            df['roi_percent'] = df['roi_percent'].apply(cap_roi)
        
        # 2. Ensure primary key columns don't have NULL values
        # Check all ID columns that are likely primary keys
        pk_columns = [col for col in df.columns if col.lower().endswith('_id') and col.lower() in [f'{table_name}_id', 'attendance_id', 'budget_id', 'quote_id', 'deal_id', 'ticket_id']]
        for col in pk_columns:
            # Check for NULL, empty, or NaN values
            null_mask = df[col].isna() | (df[col] == None) | (df[col] == '') | (df[col].astype(str).str.strip() == 'nan')
            null_count = null_mask.sum()
            if null_count > 0:
                print(f"    ⚠️  Warning: {null_count} NULL/empty values found in {col}, filling with generated IDs...")
                # Fill NULL IDs with generated IDs based on table name
                prefix = col.upper().replace('_ID', '')[:6] if '_id' in col.lower() else table_name.upper()[:4]
                for idx, row_idx in enumerate(df[null_mask].index):
                    df.at[row_idx, col] = f"{prefix}-{str(row_idx+1).zfill(6)}"
        
        # Helper function to clean values - must be defined before use
        def clean_value(val):
            """Convert pandas NA types to None - critical for Snowflake compatibility"""
            if val is None:
                return None
            # Check for pandas NA types
            try:
                if pd.isna(val):
                    return None
            except (TypeError, ValueError):
                pass
            # Check for NaT specifically (datetime NaT)
            try:
                if pd.isna(val) or (hasattr(pd, 'NaT') and val is pd.NaT):
                    return None
            except:
                pass
            # Check for pd.NA
            try:
                if hasattr(pd, 'NA') and val is pd.NA:
                    return None
            except:
                pass
            return val
        
        # Try write_pandas first (faster for large datasets)
        try:
            success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database=database_name,
            schema=schema_name,
            auto_create_table=False,  # Tables should already exist from DDL
            overwrite=True  # Overwrite existing data
        )
            return success, nrows
        except Exception as pandas_error:
            # If write_pandas fails, use cursor.execute
            # Check if it's a pandas-related error or VARIANT-related error
            error_str = str(pandas_error).lower()
            if "pandas" in error_str or "optional dependency" in error_str or "variant" in error_str or "json" in error_str:
                print(f"    ⚠️  write_pandas failed, using alternative method...", end=" ")
                cursor = conn.cursor()
                
                # Snowflake stores unquoted identifiers in UPPERCASE by default
                # Use uppercase for table name (Snowflake convention)
                table_name_upper = table_name.upper()
                
                # Use unquoted identifiers (Snowflake will uppercase them automatically)
                # First, clear the table
                try:
                    cursor.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{table_name_upper}")
                except Exception as truncate_error:
                    # If truncate fails, table might not exist or be empty - continue anyway
                    if "does not exist" not in str(truncate_error).lower():
                        print(f"    Warning: Could not truncate: {truncate_error}")
                
                # Insert data row by row (slower but more reliable)
                # Get column names - use uppercase to match Snowflake convention
                import json
                json_columns = [col for col in df.columns if 'json' in col.lower()]
                
                # Build column list
                columns = ', '.join([col.upper() for col in df.columns])
                
                # For JSON columns, PARSE_JSON doesn't work with parameterized queries
                # We need to build SQL with PARSE_JSON and embedded JSON strings (properly escaped)
                
                # Prepare data - convert JSON columns to properly formatted JSON strings
                df_for_insert = df.copy()
                for col in json_columns:
                    def json_to_string(val):
                        if val is None or pd.isna(val):
                            return None
                        # If it's already a dict/list (from convert_json_columns), convert to JSON string
                        if isinstance(val, (dict, list)):
                            return json.dumps(val, ensure_ascii=False)
                        if isinstance(val, str):
                            # If it's already a string, validate it's valid JSON
                            try:
                                # Try to parse and re-stringify to ensure it's valid JSON
                                parsed = json.loads(val)
                                return json.dumps(parsed, ensure_ascii=False)
                            except (json.JSONDecodeError, ValueError):
                                # If not valid JSON, return empty object
                                return '{}'
                        # For any other type, convert to string and wrap as JSON
                        return json.dumps(str(val), ensure_ascii=False) if val else None
                    df_for_insert[col] = df_for_insert[col].apply(json_to_string)
                
                # Clean all columns
                for col in df_for_insert.columns:
                    df_for_insert[col] = df_for_insert[col].apply(clean_value)
                
                # Build and execute INSERT statements in batches for better performance
                # Use PARSE_JSON for JSON columns with embedded strings, parameterized for others
                total_rows = 0
                batch_size = 1000  # Larger batches for better performance
                
                # If no JSON columns, use executemany for much faster bulk inserts
                if not json_columns:
                    # Fast path: no JSON columns, use executemany
                    placeholders = ', '.join(['%s'] * len(df.columns))
                    insert_sql = f"INSERT INTO {database_name}.{schema_name}.{table_name_upper} ({columns}) VALUES ({placeholders})"
                    
                    # Convert to tuples, replacing None with NULL handling
                    data_tuples = []
                    for _, row in df_for_insert.iterrows():
                        row_tuple = tuple(clean_value(val) for val in row)
                        data_tuples.append(row_tuple)
                    
                    # Execute in batches
                    for i in range(0, len(data_tuples), batch_size):
                        batch = data_tuples[i:i+batch_size]
                        cursor.executemany(insert_sql, batch)
                        total_rows += len(batch)
                    
                    conn.commit()
                else:
                    # Slower path: has JSON columns, need to insert row-by-row
                    # Snowflake doesn't support PARSE_JSON() in multi-row VALUES clauses
                    # So we insert one row at a time with proper JSON handling
                    
                    total_rows = 0
                    rows_per_commit = 100  # Commit every 100 rows
                    
                    # Use INSERT INTO ... SELECT with PARSE_JSON for JSON columns
                    # Snowflake allows PARSE_JSON in SELECT but not in VALUES clause
                    # Batch multiple rows together using UNION ALL for better performance
                    total_rows = 0
                    batch_size = 1000  # Insert 100 rows per SQL statement
                    
                    def format_value_for_sql(val, is_json=False):
                        """Format a value for SQL embedding"""
                        if val is None or val == '':
                            return 'NULL'
                        if is_json:
                            # For JSON columns, use PARSE_JSON in SELECT
                            json_str = str(val)
                            # Escape backslashes first, then single quotes for SQL
                            json_str = json_str.replace("\\", "\\\\").replace("'", "''")
                            # Also escape newlines and other control characters
                            json_str = json_str.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
                            return f"PARSE_JSON('{json_str}')"
                        elif isinstance(val, str):
                            # Escape string values for SQL
                            escaped_val = str(val).replace("'", "''").replace("\\", "\\\\")
                            return f"'{escaped_val}'"
                        elif isinstance(val, (int, float)):
                            return str(val)
                        elif isinstance(val, bool):
                            return 'TRUE' if val else 'FALSE'
                        else:
                            # For dates and other types, convert to string and escape
                            escaped_val = str(val).replace("'", "''")
                            return f"'{escaped_val}'"
                    
                    # Process in batches
                    for i in range(0, len(df_for_insert), batch_size):
                        batch_df = df_for_insert.iloc[i:i+batch_size]
                        select_statements = []
                        
                        for idx, row in batch_df.iterrows():
                            select_parts = []
                            for col in df.columns:
                                val = clean_value(row[col])
                                is_json = col in json_columns
                                select_parts.append(format_value_for_sql(val, is_json))
                            
                            select_values = ', '.join(select_parts)
                            select_statements.append(f"SELECT {select_values}")
                        
                        # Build multi-row INSERT using UNION ALL
                        union_all_sql = ' UNION ALL '.join(select_statements)
                        insert_sql = f"INSERT INTO {database_name}.{schema_name}.{table_name_upper} ({columns}) {union_all_sql}"
                        
                        # Execute the batched INSERT ... SELECT statement
                        cursor.execute(insert_sql)
                        total_rows += len(batch_df)
                        
                        # Commit every 10 batches to avoid long transactions
                        if (i // batch_size) % 10 == 0:
                            conn.commit()
                            print(f"    ... loaded {total_rows:,} rows", end="\r")
                    
                    # Final commit
                    conn.commit()
                
                cursor.close()
                return True, total_rows
            else:
                # Re-raise if it's not a pandas-related error
                raise pandas_error
        
    except Exception as e:
        print(f"    ✗ Error: {str(e)}")
        return False, 0

# ============================================================================
# Main Loading Process
# ============================================================================

def main():
    print("="*80)
    print("Sales Analytics Demo - Snowflake Data Loader")
    print("="*80)
    
    # Validate configuration
    if (SNOWFLAKE_CONFIG['user'] == 'YOUR_USERNAME' or 
        SNOWFLAKE_CONFIG['password'] == 'YOUR_PASSWORD' or 
        SNOWFLAKE_CONFIG['account'] == 'YOUR_ACCOUNT'):
        print("\n❌ ERROR: Please set Snowflake environment variables!")
        print("\n   Set these environment variables before running:")
        print("   export SNOWFLAKE_USER='your_username'")
        print("   export SNOWFLAKE_PASSWORD='your_password'")
        print("   export SNOWFLAKE_ACCOUNT='your_account'")
        print("   export SNOWFLAKE_WAREHOUSE='your_warehouse'")
        print("\n   Optional (with defaults):")
        print("   export SNOWFLAKE_DATABASE='sales_analytics_demo'")
        print("   export SNOWFLAKE_SCHEMA='PUBLIC'")
        print("\n   Or create a .env file and use: python3 -m pip install python-dotenv")
        return
    
    # Check if data directory exists
    if not os.path.exists(DATA_DIR):
        print(f"\n❌ ERROR: Data directory '{DATA_DIR}' not found!")
        print("   Please run the generator scripts first:")
        print("   python3 sales_data_generator_part1.py")
        print("   python3 sales_data_generator_part2.py")
        print("   python3 sales_data_generator_part3.py")
        print("   python3 sales_data_generator_part4.py")
        return
    
    # Connect to Snowflake
    print("\n📡 Connecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected successfully!")
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        print("\n   Common issues:")
        print("   - Incorrect account identifier (check format: xy12345.us-east-1)")
        print("   - Wrong username or password")
        print("   - Network/firewall issues")
        return
    
    # Verify database exists
    print(f"\n🔍 Verifying database '{SNOWFLAKE_CONFIG['database']}' exists...")
    try:
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")
        print("✅ Database and warehouse verified!")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print(f"\n   Please run the DDL script first:")
        print(f"   1. Open snowflake_ddl_script.sql in Snowflake Web UI")
        print(f"   2. Execute it to create database and tables")
        cursor.close()
        conn.close()
        return
    
    # Map schema directory names to Snowflake schema names
    schema_mapping = {
        'sales_data': 'sales_data',
        'usage_data': 'usage_data',
        'marketing_data': 'marketing_data',
        'support_data': 'support_data',
        'operational_data': 'operational_data'
    }
    
    # Get all CSV files
    print(f"\n📂 Scanning for CSV files in '{DATA_DIR}'...")
    csv_files = glob.glob(f'{DATA_DIR}/**/*.csv', recursive=True)
    
    if not csv_files:
        print(f"❌ No CSV files found in '{DATA_DIR}'!")
        print("   Please run the generator scripts first.")
        cursor.close()
        conn.close()
        return
    
    print(f"✅ Found {len(csv_files)} CSV files")
    
    # Load all tables
    print("\n" + "="*80)
    print("Loading data into Snowflake...")
    print("="*80)
    
    total_tables = 0
    total_rows = 0
    failed_tables = []
    
    for csv_file in sorted(csv_files):
        # Parse schema and table name from path
        path_parts = Path(csv_file).parts
        schema_dir = path_parts[-2]  # e.g., 'sales_data'
        table_name = Path(csv_file).stem  # e.g., 'customers'
        
        if schema_dir not in schema_mapping:
            print(f"⚠️  Skipping {csv_file} - unknown schema directory")
            continue
        
        snowflake_schema = schema_mapping[schema_dir]
        
        print(f"\n📄 Loading {snowflake_schema}.{table_name}...", end=" ")
        
        success, nrows = load_table(
            conn=conn,
            csv_file=csv_file,
            schema_name=snowflake_schema,
            table_name=table_name,
            database_name=SNOWFLAKE_CONFIG['database']
        )
        
        if success:
            print(f"✓ {nrows:,} rows loaded")
            total_tables += 1
            total_rows += nrows
        else:
            print(f"✗ Failed")
            failed_tables.append(f"{snowflake_schema}.{table_name}")
    
    # Summary
    print("\n" + "="*80)
    print("Loading Summary")
    print("="*80)
    print(f"✅ Successfully loaded: {total_tables} tables")
    print(f"📊 Total rows loaded: {total_rows:,}")
    
    if failed_tables:
        print(f"\n⚠️  Failed tables ({len(failed_tables)}):")
        for table in failed_tables:
            print(f"   - {table}")
    
    # Verification queries
    print("\n" + "="*80)
    print("Running verification queries...")
    print("="*80)
    
    verification_queries = [
        ("Customers", "SELECT COUNT(*) FROM sales_analytics_demo.sales_data.customers"),
        ("Products", "SELECT COUNT(*) FROM sales_analytics_demo.sales_data.products"),
        ("Opportunities", "SELECT COUNT(*) FROM sales_analytics_demo.sales_data.opportunities"),
        ("Closed Deals", "SELECT COUNT(*) FROM sales_analytics_demo.sales_data.closed_deals"),
        ("Support Tickets", "SELECT COUNT(*) FROM sales_analytics_demo.support_data.support_tickets"),
        ("Campaigns", "SELECT COUNT(*) FROM sales_analytics_demo.marketing_data.campaigns"),
        ("Leads", "SELECT COUNT(*) FROM sales_analytics_demo.marketing_data.leads"),
        ("Product Usage", "SELECT COUNT(*) FROM sales_analytics_demo.usage_data.product_usage_summary"),
    ]
    
    for name, query in verification_queries:
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            print(f"  {name:20s}: {result[0]:>8,} rows")
        except Exception as e:
            print(f"  {name:20s}: Error - {str(e)}")
    
    # Check all schemas
    print("\n" + "="*80)
    print("Schema Summary")
    print("="*80)
    
    try:
        cursor.execute("""
            SELECT table_schema, COUNT(*) as table_count
            FROM information_schema.tables
            WHERE table_catalog = 'SALES_ANALYTICS_DEMO'
              AND table_schema IN ('SALES_DATA', 'USAGE_DATA', 'MARKETING_DATA', 
                                   'SUPPORT_DATA', 'OPERATIONAL_DATA')
            GROUP BY table_schema
            ORDER BY table_schema
        """)
        
        results = cursor.fetchall()
        for schema, count in results:
            print(f"  {schema:20s}: {count:>3} tables")
    except Exception as e:
        print(f"  Error checking schemas: {str(e)}")
    
    # Close connection
    cursor.close()
    conn.close()
    
    print("\n" + "="*80)
    print("✅ Loading Complete!")
    print("="*80)
    print("\nNext steps:")
    print("  1. Run sample queries from complete_readme.md")
    print("  2. Create views for common queries")
    print("  3. Set up dashboards in Snowsight or BI tools")
    print("\n")

if __name__ == "__main__":
    main()

