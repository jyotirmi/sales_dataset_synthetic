#!/usr/bin/env python3
"""
Quick script to load specific tables into Snowflake
Useful for testing and debugging individual tables

Usage: 
    1. Set environment variables (see .env.example)
    2. Edit the tables_to_load list below
    3. Run: python3 load_specific_tables.py
"""

import sys
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

# Add parent directory to path to import from load_to_snowflake
sys.path.insert(0, str(Path(__file__).parent))

# Import the loader function and config from the main script
from load_to_snowflake import load_table, SNOWFLAKE_CONFIG, DATA_DIR
import snowflake.connector

def main():
    # Tables to load - edit this list to test specific tables
    tables_to_load = [
        ('sales_data', 'products')
    ]
    
    print("="*80)
    print("Loading Specific Tables to Snowflake")
    print("="*80)
    print(f"\n📋 Tables to load:")
    for schema, table in tables_to_load:
        print(f"   - {schema}.{table}")
    
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
        return
    
    # Connect to Snowflake
    print("\n📡 Connecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected successfully!")
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return
    
    # Verify database exists
    print(f"\n🔍 Verifying database '{SNOWFLAKE_CONFIG['database']}' exists...")
    try:
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")
        print("✅ Database and warehouse verified!")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        cursor.close()
        conn.close()
        return
    
    # Load the specific tables
    print("\n" + "="*80)
    print("Loading tables...")
    print("="*80)
    
    total_rows = 0
    failed_tables = []
    
    for schema_name, table_name in tables_to_load:
        csv_file = os.path.join(DATA_DIR, schema_name, f"{table_name}.csv")
        
        if not os.path.exists(csv_file):
            print(f"\n❌ File not found: {csv_file}")
            failed_tables.append(f"{schema_name}.{table_name}")
            continue
        
        print(f"\n📄 Loading {schema_name}.{table_name}...", end=" ")
        
        success, nrows = load_table(
            conn=conn,
            csv_file=csv_file,
            schema_name=schema_name,
            table_name=table_name,
            database_name=SNOWFLAKE_CONFIG['database']
        )
        
        if success:
            print(f"✅ Loaded {nrows:,} rows")
            total_rows += nrows
        else:
            print("✗ Failed")
            failed_tables.append(f"{schema_name}.{table_name}")
    
    # Summary
    print("\n" + "="*80)
    print("Summary")
    print("="*80)
    print(f"✅ Successfully loaded: {len(tables_to_load) - len(failed_tables)}/{len(tables_to_load)} tables")
    print(f"📊 Total rows loaded: {total_rows:,}")
    
    if failed_tables:
        print(f"\n❌ Failed tables ({len(failed_tables)}):")
        for table in failed_tables:
            print(f"   - {table}")
    else:
        print("\n🎉 All tables loaded successfully!")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()

