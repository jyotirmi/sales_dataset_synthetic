"""
Sales Analytics Demo Dataset Generator - Part 2
Generates additional tables (30-35, 38-50, 51-52, 54-56, 57-59, 61-62)
Run this after Part 1
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
# Get project root directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "sales_analytics_data")
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 9, 30)

# Load existing data from Part 1
print("Loading data from Part 1...")
customers_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/customers.csv")
products_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/products.csv")
customer_products_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/customer_products.csv")
opportunities_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/opportunities.csv")
support_tickets_df = pd.read_csv(f"{OUTPUT_DIR}/support_data/support_tickets.csv")
campaigns_df = pd.read_csv(f"{OUTPUT_DIR}/marketing_data/campaigns.csv")
leads_df = pd.read_csv(f"{OUTPUT_DIR}/marketing_data/leads.csv")
customer_contacts_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/customer_contacts.csv")

print("Data loaded successfully!")

def random_date(start, end):
    """Generate random date between start and end"""
    if isinstance(start, str):
        start = datetime.strptime(start, '%Y-%m-%d')
    if isinstance(end, str):
        end = datetime.strptime(end, '%Y-%m-%d')
    delta = end - start
    random_days = random.randint(0, max(1, delta.days))
    return start + timedelta(days=random_days)

def generate_id(prefix, num):
    """Generate ID with prefix"""
    return f"{prefix}-{str(num).zfill(6)}"

print("\n=== Generating Additional Sales Tables ===")

# 30. ACCOUNT_HIERARCHY
print("Generating ACCOUNT_HIERARCHY table...")
hierarchy_data = []
hierarchy_id = 1

# Create parent-child relationships for ~20% of customers
parent_candidates = customers_df.sample(n=int(len(customers_df) * 0.1))
for _, parent in parent_candidates.iterrows():
    # Each parent has 1-5 children
    num_children = random.randint(1, 5)
    children = customers_df[customers_df['customer_id'] != parent['customer_id']].sample(n=min(num_children, 10))
    
    for _, child in children.iterrows():
        hierarchy_data.append({
            'hierarchy_id': generate_id('HIER', hierarchy_id),
            'parent_customer_id': parent['customer_id'],
            'child_customer_id': child['customer_id'],
            'relationship_type': random.choice(['Parent-Child', 'Sister Company', 'Division', 'Franchise']),
            'ownership_percent': round(random.uniform(51, 100), 2) if random.random() > 0.5 else None,
            'created_date': random_date(max(parent['created_date'], child['created_date']), END_DATE)
        })
        hierarchy_id += 1

hierarchy_df = pd.DataFrame(hierarchy_data)
hierarchy_df.to_csv(f"{OUTPUT_DIR}/sales_data/account_hierarchy.csv", index=False)
print(f"✓ Generated {len(hierarchy_df)} account hierarchy records")

# 31. CUSTOMER_NOTES
print("Generating CUSTOMER_NOTES table...")
notes_data = []
note_types = ['Meeting', 'Call', 'Email', 'Internal', 'Other']

for i in range(5000):
    customer = customers_df.sample(1).iloc[0]
    contact = customer_contacts_df[customer_contacts_df['customer_id'] == customer['customer_id']].sample(1).iloc[0] if random.random() > 0.3 else None
    
    notes_data.append({
        'note_id': generate_id('NOTE', i+1),
        'customer_id': customer['customer_id'],
        'contact_id': contact['contact_id'] if contact is not None else None,
        'note_date': random_date(customer['created_date'], END_DATE),
        'note_type': random.choice(note_types),
        'created_by': customer['account_owner'],
        'subject': f"Note about {customer['customer_name']}",
        'note_text': f"Detailed notes for customer interaction #{i+1}",
        'opportunity_id': random.choice(list(opportunities_df['opportunity_id'])) if random.random() > 0.7 else None,
        'next_action': random.choice(['Follow up', 'Send proposal', 'Schedule demo', 'Review contract', None]),
        'next_action_date': random_date(START_DATE, END_DATE) if random.random() > 0.5 else None
    })

notes_df = pd.DataFrame(notes_data)
notes_df.to_csv(f"{OUTPUT_DIR}/sales_data/customer_notes.csv", index=False)
print(f"✓ Generated {len(notes_df)} customer notes")

# 32. COMPETITIVE_INTELLIGENCE
print("Generating COMPETITIVE_INTELLIGENCE table...")
intel_data = []
competitors = ['Competitor A', 'Competitor B', 'Competitor C', 'Competitor D', 'Competitor E']
comp_products = ['CompProd X', 'CompProd Y', 'CompProd Z']

for i in range(2000):
    customer = customers_df.sample(1).iloc[0]
    opp = opportunities_df.sample(1).iloc[0] if random.random() > 0.5 else None
    
    intel_data.append({
        'intel_id': generate_id('INTEL', i+1),
        'customer_id': customer['customer_id'],
        'opportunity_id': opp['opportunity_id'] if opp is not None else None,
        'competitor_name': random.choice(competitors),
        'competitor_product': random.choice(comp_products),
        'intel_date': random_date(customer['created_date'], END_DATE),
        'intel_source': random.choice(['Sales call', 'Customer', 'Market research', 'Partner', 'Public info']),
        'competitor_strength': random.choice(['Price', 'Features', 'Brand', 'Support', 'Integration']),
        'competitor_weakness': random.choice(['Price', 'Complexity', 'Support', 'Scalability', 'Integration']),
        'our_position': random.choice(['Winning', 'Winning', 'At Risk', 'Lost']),
        'intel_type': random.choice(['Product comparison', 'Pricing', 'Customer feedback']),
        'notes': f"Competitive intelligence #{i+1}"
    })

intel_df = pd.DataFrame(intel_data)
intel_df.to_csv(f"{OUTPUT_DIR}/sales_data/competitive_intelligence.csv", index=False)
print(f"✓ Generated {len(intel_df)} competitive intelligence records")

# 33. QUOTES
print("Generating QUOTES table...")
quotes_data = []

for i in range(3000):
    opp = opportunities_df.sample(1).iloc[0]
    quote_date = random_date(opp['created_date'], opp['close_date'])
    
    quotes_data.append({
        'quote_id': generate_id('QUOT', i+1),
        'quote_number': f"QUOT-{str(i+1).zfill(6)}",
        'opportunity_id': opp['opportunity_id'],
        'customer_id': opp['customer_id'],
        'quote_date': quote_date,
        'expiration_date': quote_date + timedelta(days=30),
        'total_amount': round(opp['amount'] * random.uniform(0.9, 1.1), 2),
        'discount_percent': round(random.uniform(0, 25), 2),
        'payment_terms': random.choice(['Net 30', 'Net 60', 'Net 90', 'Due on Receipt']),
        'delivery_terms': random.choice(['FOB', 'CIF', 'Digital Delivery', 'Cloud Service']),
        'status': random.choice(['Draft', 'Sent', 'Accepted', 'Rejected', 'Expired']),
        'created_by': opp['sales_rep'],
        'approved_by': f"Manager_{random.randint(1, 10)}",
        'approval_date': quote_date + timedelta(days=random.randint(1, 3)),
        'notes': f"Quote notes #{i+1}"
    })

quotes_df = pd.DataFrame(quotes_data)
quotes_df.to_csv(f"{OUTPUT_DIR}/sales_data/quotes.csv", index=False)
print(f"✓ Generated {len(quotes_df)} quotes")

# 34. QUOTE_LINE_ITEMS
print("Generating QUOTE_LINE_ITEMS table...")
line_items_data = []
line_item_id = 1

for _, quote in quotes_df.iterrows():
    # Each quote has 1-5 line items
    num_items = random.randint(1, 5)
    products_in_quote = products_df.sample(n=min(num_items, len(products_df)))
    
    for _, product in products_in_quote.iterrows():
        quantity = random.randint(10, 500)
        unit_price = product['unit_price']
        discount = round(random.uniform(0, 20), 2)
        line_total = round(quantity * unit_price * (1 - discount/100), 2)
        
        line_items_data.append({
            'line_item_id': generate_id('LINE', line_item_id),
            'quote_id': quote['quote_id'],
            'product_id': product['product_id'],
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_percent': discount,
            'line_total': line_total,
            'description': f"{product['product_name']} - {product['product_line']}",
            'product_version': product['current_version'],
            'license_type': random.choice(['Perpetual', 'Subscription', 'Usage-based']),
            'term_months': random.choice([12, 24, 36])
        })
        line_item_id += 1

line_items_df = pd.DataFrame(line_items_data)
line_items_df.to_csv(f"{OUTPUT_DIR}/sales_data/quote_line_items.csv", index=False)
print(f"✓ Generated {len(line_items_df)} quote line items")

# 35. CONTRACTS
print("Generating CONTRACTS table...")
contracts_data = []

accepted_quotes = quotes_df[quotes_df['status'] == 'Accepted']
for i, (_, quote) in enumerate(accepted_quotes.iterrows()):
    start_date = random_date(quote['quote_date'], quote['expiration_date'])
    contract_length = random.choice([12, 24, 36])
    
    contracts_data.append({
        'contract_id': generate_id('CONT', i+1),
        'contract_number': f"CONT-{str(i+1).zfill(6)}",
        'customer_id': quote['customer_id'],
        'opportunity_id': quote['opportunity_id'],
        'quote_id': quote['quote_id'],
        'contract_type': random.choice(['Master Agreement', 'SOW', 'Amendment', 'Renewal']),
        'start_date': start_date,
        'end_date': start_date + timedelta(days=contract_length*30),
        'contract_value': quote['total_amount'],
        'annual_value': round(quote['total_amount'] / (contract_length/12), 2),
        'payment_schedule': random.choice(['Monthly', 'Quarterly', 'Annual', 'Upfront']),
        'auto_renew': random.choice(['Yes', 'No']),
        'renewal_notice_days': random.choice([30, 60, 90]),
        'status': 'Active' if start_date <= END_DATE <= start_date + timedelta(days=contract_length*30) else 'Expired',
        'signed_date': start_date,
        'signed_by_customer': f"Customer Signer {i+1}",
        'signed_by_vendor': f"Vendor Signer {random.randint(1, 5)}"
    })

contracts_df = pd.DataFrame(contracts_data)
contracts_df.to_csv(f"{OUTPUT_DIR}/sales_data/contracts.csv", index=False)
print(f"✓ Generated {len(contracts_df)} contracts")

print("\n=== Generating Additional Usage Tables ===")

# 38. USER_SESSIONS
print("Generating USER_SESSIONS table...")
sessions_data = []

for i in range(50000):
    install = customer_products_df.sample(1).iloc[0]
    session_start = random_date(install['install_date'], END_DATE)
    duration = random.randint(5, 180)
    
    sessions_data.append({
        'session_id': generate_id('SESS', i+1),
        'customer_id': install['customer_id'],
        'product_id': install['product_id'],
        'user_id': f"user_{random.randint(1, 10000)}",
        'session_start': session_start,
        'session_end': session_start + timedelta(minutes=duration),
        'duration_minutes': duration,
        'pages_viewed': random.randint(1, 50),
        'actions_taken': random.randint(0, 100),
        'device_type': random.choice(['Desktop', 'Mobile', 'Tablet']),
        'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
        'os': random.choice(['Windows', 'macOS', 'Linux', 'iOS', 'Android']),
        'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        'location_city': random.choice(['New York', 'London', 'Tokyo', 'Sydney', 'Mumbai']),
        'location_country': random.choice(['USA', 'UK', 'Japan', 'Australia', 'India'])
    })

sessions_df = pd.DataFrame(sessions_data)
sessions_df.to_csv(f"{OUTPUT_DIR}/usage_data/user_sessions.csv", index=False)
print(f"✓ Generated {len(sessions_df)} user sessions")

# 39. API_USAGE
print("Generating API_USAGE table...")
# Load products to get usage_tier
products_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/products.csv")
customer_products_with_tier = customer_products_df.copy()

# Add usage_tier using map (more reliable than merge)
if 'usage_tier' in products_df.columns:
    tier_map = dict(zip(products_df['product_id'], products_df['usage_tier']))
    customer_products_with_tier['usage_tier'] = customer_products_with_tier['product_id'].map(tier_map)
    customer_products_with_tier['usage_tier'] = customer_products_with_tier['usage_tier'].fillna('Medium')
else:
    customer_products_with_tier['usage_tier'] = 'Medium'

api_usage_data = []
endpoints = ['/api/users', '/api/data', '/api/reports', '/api/export', '/api/analytics']
methods = ['GET', 'POST', 'PUT', 'DELETE']

usage_id = 1
for _, install in customer_products_with_tier.iterrows():
    usage_tier = install['usage_tier']
    
    # Adjust API usage based on tier
    if usage_tier == 'Low':
        call_count_range = (10, 1000)
        data_range = (0.01, 5)
    elif usage_tier == 'High':
        call_count_range = (50000, 200000)
        data_range = (50, 500)
    else:  # Medium
        call_count_range = (5000, 50000)
        data_range = (5, 50)
    
    # Generate daily API usage from install to now
    current_date = datetime.strptime(install['install_date'], '%Y-%m-%d') if isinstance(install['install_date'], str) else install['install_date']
    while current_date <= END_DATE:
        num_endpoints = random.randint(2, 4) if usage_tier != 'Low' else random.randint(1, 3)
        for endpoint in random.sample(endpoints, num_endpoints):
            call_count = random.randint(*call_count_range)
            success_count = int(call_count * random.uniform(0.95, 0.999))
            
            api_usage_data.append({
                'api_usage_id': generate_id('API', usage_id),
                'customer_id': install['customer_id'],
                'product_id': install['product_id'],
                'usage_date': current_date,
                'endpoint': endpoint,
                'method': random.choice(methods),
                'call_count': call_count,
                'success_count': success_count,
                'error_count': call_count - success_count,
                'avg_response_time_ms': round(random.uniform(50, 500), 2),
                'data_transferred_gb': round(random.uniform(*data_range), 2),
                'authentication_type': random.choice(['API Key', 'OAuth', 'Token'])
            })
            usage_id += 1
        current_date += timedelta(days=7)  # Weekly

api_usage_df = pd.DataFrame(api_usage_data)
api_usage_df.to_csv(f"{OUTPUT_DIR}/usage_data/api_usage.csv", index=False)
print(f"✓ Generated {len(api_usage_df)} API usage records")

# 40. INTEGRATION_USAGE
print("Generating INTEGRATION_USAGE table...")
integration_data = []
integrations = ['Salesforce', 'Slack', 'Microsoft Teams', 'Jira', 'GitHub', 'AWS S3', 'Google Drive']

for i, (_, install) in enumerate(customer_products_with_tier.iterrows()):
    usage_tier = install['usage_tier']
    # Adjust number of integrations based on tier
    if usage_tier == 'Low':
        num_integrations = random.randint(1, 2)
        records_range = (100, 10000)
    elif usage_tier == 'High':
        num_integrations = random.randint(3, 5)
        records_range = (50000, 500000)
    else:  # Medium
        num_integrations = random.randint(2, 4)
        records_range = (10000, 100000)
    
    for integration in random.sample(integrations, num_integrations):
        integration_data.append({
            'integration_id': generate_id('INTG', i*4 + num_integrations),
            'customer_id': install['customer_id'],
            'product_id': install['product_id'],
            'integration_name': integration,
            'integration_type': random.choice(['CRM', 'Marketing', 'Communication', 'Data']),
            'status': random.choice(['Active', 'Active', 'Active', 'Inactive', 'Error']),
            'install_date': random_date(install['install_date'], END_DATE),
            'last_sync_date': random_date(START_DATE, END_DATE),
            'sync_frequency': random.choice(['Real-time', 'Hourly', 'Daily']),
            'records_synced': random.randint(*records_range),
            'error_count': random.randint(0, 50),
            'config_json': '{}'
        })

integration_df = pd.DataFrame(integration_data)
integration_df.to_csv(f"{OUTPUT_DIR}/usage_data/integration_usage.csv", index=False)
print(f"✓ Generated {len(integration_df)} integration records")

# 41. LICENSE_USAGE
print("Generating LICENSE_USAGE table...")
license_usage_data = []
usage_id = 1

for _, install in customer_products_with_tier.iterrows():
    usage_tier = install['usage_tier']
    license_count = install['license_count']
    
    # Adjust license utilization based on tier
    if usage_tier == 'Low':
        min_util = 0.05  # 5%
        max_util = 0.30  # 30%
    elif usage_tier == 'High':
        min_util = 0.70  # 70%
        max_util = 0.98  # 98%
    else:  # Medium
        min_util = 0.30  # 30%
        max_util = 0.70  # 70%
    
    current_date = datetime.strptime(install['install_date'], '%Y-%m-%d') if isinstance(install['install_date'], str) else install['install_date']
    while current_date <= END_DATE:
        licenses_active = random.randint(
            max(1, int(license_count * min_util)), 
            max(1, int(license_count * max_util))
        )
        utilization = round((licenses_active / license_count) * 100, 2)
        
        license_usage_data.append({
            'license_usage_id': generate_id('LIC', usage_id),
            'customer_id': install['customer_id'],
            'product_id': install['product_id'],
            'usage_date': current_date,
            'licenses_purchased': license_count,
            'licenses_active': licenses_active,
            'licenses_available': license_count - licenses_active,
            'utilization_percent': utilization,
            'peak_usage_date': current_date,
            'peak_usage_count': min(licenses_active + random.randint(0, 10), license_count),
            'overage_count': max(0, licenses_active - license_count),
            'overage_charges': round(max(0, licenses_active - license_count) * 50, 2)
        })
        usage_id += 1
        current_date += timedelta(days=30)  # Monthly

license_usage_df = pd.DataFrame(license_usage_data)
license_usage_df.to_csv(f"{OUTPUT_DIR}/usage_data/license_usage.csv", index=False)
print(f"✓ Generated {len(license_usage_df)} license usage records")

print(f"\n✓ Part 2 Generation Complete!")
print(f"Generated {len(hierarchy_df) + len(notes_df) + len(intel_df) + len(quotes_df) + len(line_items_df) + len(contracts_df) + len(sessions_df) + len(api_usage_df) + len(integration_df) + len(license_usage_df):,} additional records")
print("\nContinue with Part 3 for remaining tables...")