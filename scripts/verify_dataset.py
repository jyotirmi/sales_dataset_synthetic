"""
Sales Analytics Demo Dataset - Verification Script
Verifies that all 62 tables have been generated correctly
"""

import os
import pandas as pd
from pathlib import Path

import os
# Get project root directory (parent of scripts/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "sales_analytics_data")

# Define all expected tables by schema
EXPECTED_TABLES = {
    'sales_data': [
        'customers', 'products', 'customer_products', 'opportunities',
        'closed_deals', 'sales_targets', 'customer_attributes',
        'customer_contacts', 'account_hierarchy', 'customer_notes',
        'competitive_intelligence', 'quotes', 'quote_line_items',
        'contracts', 'revenue_recognition', 'territories'
    ],
    'usage_data': [
        'product_usage_summary', 'feature_usage', 'telemetry_events',
        'product_health_alerts', 'user_sessions', 'api_usage',
        'integration_usage', 'license_usage', 'data_quality_metrics',
        'performance_metrics'
    ],
    'marketing_data': [
        'campaigns', 'events', 'leads', 'campaign_engagement',
        'marketing_attribution', 'webinar_attendance', 'content_assets',
        'email_campaigns', 'social_media_engagement', 'website_traffic',
        'partner_referrals', 'marketing_budget', 'lead_scoring_rules'
    ],
    'support_data': [
        'support_tickets', 'ticket_comments', 'ticket_resolution',
        'customer_satisfaction', 'sla_policies', 'sla_breaches',
        'knowledge_base', 'ticket_tags', 'escalations',
        'recurring_issues', 'support_agent_metrics', 'support_channels',
        'ticket_worklog'
    ],
    'operational_data': [
        'customer_health_score', 'onboarding_progress', 'product_releases',
        'customer_success_activities', 'user_accounts', 'audit_log',
        'notifications', 'invoices', 'payments', 'renewals'
    ]
}

def verify_dataset():
    """Verify all tables exist and contain data"""
    
    print("="*80)
    print("SALES ANALYTICS DATASET VERIFICATION")
    print("="*80)
    
    total_tables = 0
    total_records = 0
    missing_tables = []
    empty_tables = []
    table_stats = {}
    
    # Check each schema
    for schema, tables in EXPECTED_TABLES.items():
        print(f"\n{'='*80}")
        print(f"Checking {schema.upper()} Schema")
        print(f"{'='*80}")
        
        schema_path = Path(OUTPUT_DIR) / schema
        
        if not schema_path.exists():
            print(f"❌ Schema directory not found: {schema_path}")
            missing_tables.extend([f"{schema}/{table}" for table in tables])
            continue
        
        for table in tables:
            total_tables += 1
            file_path = schema_path / f"{table}.csv"
            
            if not file_path.exists():
                print(f"❌ Missing: {table}.csv")
                missing_tables.append(f"{schema}/{table}")
                continue
            
            try:
                df = pd.read_csv(file_path)
                record_count = len(df)
                column_count = len(df.columns)
                
                if record_count == 0:
                    print(f"⚠️  Empty: {table}.csv (0 records)")
                    empty_tables.append(f"{schema}/{table}")
                else:
                    print(f"✓ {table}.csv: {record_count:,} records, {column_count} columns")
                    total_records += record_count
                    table_stats[f"{schema}/{table}"] = {
                        'records': record_count,
                        'columns': column_count,
                        'size_mb': file_path.stat().st_size / (1024 * 1024)
                    }
            
            except Exception as e:
                print(f"❌ Error reading {table}.csv: {str(e)}")
                missing_tables.append(f"{schema}/{table}")
    
    # Summary Report
    print("\n" + "="*80)
    print("VERIFICATION SUMMARY")
    print("="*80)
    
    print(f"\nExpected tables: 62")
    print(f"Found tables: {total_tables - len(missing_tables)}")
    print(f"Missing tables: {len(missing_tables)}")
    print(f"Empty tables: {len(empty_tables)}")
    print(f"\nTotal records across all tables: {total_records:,}")
    
    if missing_tables:
        print(f"\n❌ Missing Tables ({len(missing_tables)}):")
        for table in missing_tables:
            print(f"   - {table}")
    
    if empty_tables:
        print(f"\n⚠️  Empty Tables ({len(empty_tables)}):")
        for table in empty_tables:
            print(f"   - {table}")
    
    # Top 10 largest tables
    if table_stats:
        print(f"\n📊 Top 10 Largest Tables (by record count):")
        sorted_tables = sorted(table_stats.items(), key=lambda x: x[1]['records'], reverse=True)[:10]
        for i, (table, stats) in enumerate(sorted_tables, 1):
            print(f"   {i}. {table}: {stats['records']:,} records ({stats['size_mb']:.2f} MB)")
    
    # Schema breakdown
    print(f"\n📁 Records by Schema:")
    for schema, tables in EXPECTED_TABLES.items():
        schema_records = sum(
            table_stats.get(f"{schema}/{table}", {}).get('records', 0) 
            for table in tables
        )
        schema_tables = sum(
            1 for table in tables 
            if f"{schema}/{table}" in table_stats
        )
        print(f"   {schema}: {schema_records:,} records across {schema_tables}/{len(tables)} tables")
    
    # Verification status
    print(f"\n{'='*80}")
    if len(missing_tables) == 0 and len(empty_tables) == 0:
        print("✅ VERIFICATION PASSED - All 62 tables present with data!")
        print("="*80)
        print("\nNext Steps:")
        print("1. Upload to S3: aws s3 sync sales_analytics_data/ s3://your-bucket/sales-demo-data/")
        print("2. Load into Snowflake using: snowflake_ddl_script.sql")
        print("3. Load into Databricks using: databricks_loader.py notebook")
        return True
    else:
        print("❌ VERIFICATION FAILED - Issues found")
        print("="*80)
        print("\nPlease run the missing generator scripts:")
        if any('sales_data' in t or 'usage_data' in t or 'marketing_data' in t or 'support_data' in t for t in missing_tables[:20]):
            print("   - python sales_data_generator_part1.py")
            print("   - python sales_data_generator_part2.py")
            print("   - python sales_data_generator_part3.py")
        print("   - python sales_data_generator_part4.py")
        return False

def check_data_quality():
    """Perform basic data quality checks"""
    
    print("\n" + "="*80)
    print("DATA QUALITY CHECKS")
    print("="*80)
    
    checks_passed = 0
    checks_failed = 0
    
    try:
        # Check 1: Customers have unique IDs
        customers_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/customers.csv")
        if customers_df['customer_id'].is_unique:
            print("✓ All customer IDs are unique")
            checks_passed += 1
        else:
            print("❌ Duplicate customer IDs found")
            checks_failed += 1
        
        # Check 2: Products have unique IDs
        products_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/products.csv")
        if products_df['product_id'].is_unique:
            print("✓ All product IDs are unique")
            checks_passed += 1
        else:
            print("❌ Duplicate product IDs found")
            checks_failed += 1
        
        # Check 3: Opportunities reference valid customers
        opportunities_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/opportunities.csv")
        valid_customers = set(customers_df['customer_id'])
        invalid_refs = opportunities_df[~opportunities_df['customer_id'].isin(valid_customers)]
        if len(invalid_refs) == 0:
            print("✓ All opportunities reference valid customers")
            checks_passed += 1
        else:
            print(f"❌ {len(invalid_refs)} opportunities reference invalid customers")
            checks_failed += 1
        
        # Check 4: Closed deals reference valid opportunities
        closed_deals_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/closed_deals.csv")
        valid_opps = set(opportunities_df['opportunity_id'])
        invalid_deals = closed_deals_df[~closed_deals_df['opportunity_id'].isin(valid_opps)]
        if len(invalid_deals) == 0:
            print("✓ All closed deals reference valid opportunities")
            checks_passed += 1
        else:
            print(f"❌ {len(invalid_deals)} closed deals reference invalid opportunities")
            checks_failed += 1
        
        # Check 5: Support tickets reference valid customers
        support_tickets_df = pd.read_csv(f"{OUTPUT_DIR}/support_data/support_tickets.csv")
        invalid_tickets = support_tickets_df[~support_tickets_df['customer_id'].isin(valid_customers)]
        if len(invalid_tickets) == 0:
            print("✓ All support tickets reference valid customers")
            checks_passed += 1
        else:
            print(f"❌ {len(invalid_tickets)} support tickets reference invalid customers")
            checks_failed += 1
        
        # Check 6: Date ranges are reasonable
        if pd.to_datetime(customers_df['created_date']).min() >= pd.Timestamp('2023-01-01'):
            print("✓ Customer dates are within expected range")
            checks_passed += 1
        else:
            print("❌ Some customer dates are before expected start date")
            checks_failed += 1
        
        print(f"\n{'='*80}")
        print(f"Quality Checks: {checks_passed} passed, {checks_failed} failed")
        print("="*80)
        
        return checks_failed == 0
        
    except Exception as e:
        print(f"❌ Error during quality checks: {str(e)}")
        return False

if __name__ == "__main__":
    print("\n")
    verification_passed = verify_dataset()
    
    if verification_passed:
        quality_passed = check_data_quality()
        
        if quality_passed:
            print("\n🎉 SUCCESS! Dataset is complete and passes all quality checks!")
        else:
            print("\n⚠️  Dataset is complete but has quality issues")
    else:
        print("\n❌ Dataset is incomplete. Please run missing generator scripts.")
    
    print("\n")