"""
Sales Analytics Demo Dataset Generator
Generates 62 tables with realistic synthetic data for Snowflake/Databricks demo
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration - Get project root directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "sales_analytics_data")
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 18
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 9, 30)

# Create output directory structure
os.makedirs(f"{OUTPUT_DIR}/sales_data", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/usage_data", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/marketing_data", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/support_data", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/operational_data", exist_ok=True)

print("Starting data generation...")
print(f"Output directory: {OUTPUT_DIR}")

# Helper functions
def random_date(start, end):
    """Generate random date between start and end"""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

def generate_id(prefix, num):
    """Generate ID with prefix"""
    return f"{prefix}-{str(num).zfill(6)}"

# Reference data
INDUSTRIES = ['Technology', 'Healthcare', 'Finance', 'Retail', 'Manufacturing', 
              'Education', 'Media', 'Telecommunications', 'Energy', 'Transportation']
COMPANY_SIZES = ['Small', 'Medium', 'Large', 'Enterprise']
REGIONS = ['North America', 'EMEA', 'APAC', 'Latin America']
COUNTRIES = {
    'North America': ['United States', 'Canada', 'Mexico'],
    'EMEA': ['United Kingdom', 'Germany', 'France', 'Spain', 'UAE'],
    'APAC': ['Japan', 'Australia', 'Singapore', 'India', 'China'],
    'Latin America': ['Brazil', 'Argentina', 'Chile', 'Colombia']
}
CUSTOMER_TIERS = ['Bronze', 'Silver', 'Gold', 'Platinum']
PRODUCT_CATEGORIES = ['Analytics', 'Security', 'Collaboration', 'Integration', 'Storage']

print("\n=== Generating Sales & Customer Data ===")

# 1. CUSTOMERS
print("Generating CUSTOMERS table...")
customers_data = []
for i in range(NUM_CUSTOMERS):
    region = random.choice(REGIONS)
    customers_data.append({
        'customer_id': generate_id('CUST', i+1),
        'customer_name': f"Company {chr(65 + i % 26)}{i+1}",
        'industry': random.choice(INDUSTRIES),
        'company_size': random.choice(COMPANY_SIZES),
        'region': region,
        'country': random.choice(COUNTRIES[region]),
        'annual_revenue': round(random.uniform(1000000, 500000000), 2),
        'account_owner': f"Rep_{random.randint(1, 50)}",
        'created_date': random_date(START_DATE, END_DATE - timedelta(days=180)),
        'customer_tier': random.choice(CUSTOMER_TIERS)
    })
customers_df = pd.DataFrame(customers_data)
customers_df.to_csv(f"{OUTPUT_DIR}/sales_data/customers.csv", index=False)
print(f"✓ Generated {len(customers_df)} customers")

# 2. PRODUCTS
print("Generating PRODUCTS table...")
# Define usage tiers - some products will have low, medium, or high usage patterns
# Distribution: 30% low usage, 40% medium usage, 30% high usage
usage_tiers = ['Low', 'Low', 'Low', 'Medium', 'Medium', 'Medium', 'Medium', 'High', 'High', 'High']
products_data = []
for i in range(NUM_PRODUCTS):
    # Assign usage tier (cycling through to ensure distribution)
    usage_tier = usage_tiers[i % len(usage_tiers)]
    products_data.append({
        'product_id': generate_id('PROD', i+1),
        'product_name': f"Product {chr(65 + i)}",
        'product_category': random.choice(PRODUCT_CATEGORIES),
        'current_version': f"{random.randint(2,5)}.{random.randint(0,9)}.{random.randint(0,20)}",
        'unit_price': round(random.uniform(1000, 50000), 2),
        'product_line': random.choice(['Core', 'Premium', 'Enterprise']),
        'release_date': random_date(START_DATE - timedelta(days=730), START_DATE),
        'usage_tier': usage_tier  # Internal field for usage generation
    })
products_df = pd.DataFrame(products_data)
products_df.to_csv(f"{OUTPUT_DIR}/sales_data/products.csv", index=False)
print(f"✓ Generated {len(products_df)} products")
print(f"   Usage tiers: {products_df['usage_tier'].value_counts().to_dict()}")

# 3. CUSTOMER_PRODUCTS (Install Base)
print("Generating CUSTOMER_PRODUCTS table...")
customer_products_data = []
install_id = 1
for _, customer in customers_df.iterrows():
    # Each customer has 1-4 products
    num_products = random.randint(1, 4)
    customer_product_ids = random.sample(list(products_df['product_id']), num_products)
    
    for prod_id in customer_product_ids:
        product = products_df[products_df['product_id'] == prod_id].iloc[0]
        install_date = random_date(customer['created_date'], END_DATE)
        
        customer_products_data.append({
            'install_id': generate_id('INST', install_id),
            'customer_id': customer['customer_id'],
            'product_id': prod_id,
            'version_number': product['current_version'] if random.random() > 0.3 else f"{random.randint(1,4)}.{random.randint(0,9)}.{random.randint(0,20)}",
            'license_count': random.randint(10, 1000),
            'install_date': install_date,
            'contract_value': round(random.uniform(10000, 500000), 2),
            'renewal_date': install_date + timedelta(days=365),
            'usage_tier': random.choice(['Low', 'Medium', 'High']),
            'support_level': random.choice(['Basic', 'Standard', 'Premium'])
        })
        install_id += 1

customer_products_df = pd.DataFrame(customer_products_data)
customer_products_df.to_csv(f"{OUTPUT_DIR}/sales_data/customer_products.csv", index=False)
print(f"✓ Generated {len(customer_products_df)} customer product installations")

# 4. OPPORTUNITIES
print("Generating OPPORTUNITIES table...")
opportunities_data = []
stages = ['Prospecting', 'Qualification', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost']
opp_types = ['New Business', 'Upsell', 'Cross-sell', 'Renewal']

for i in range(4000):
    customer = customers_df.sample(1).iloc[0]
    product = products_df.sample(1).iloc[0]
    created = random_date(START_DATE, END_DATE)
    stage = random.choice(stages)
    
    opportunities_data.append({
        'opportunity_id': generate_id('OPP', i+1),
        'customer_id': customer['customer_id'],
        'product_id': product['product_id'],
        'opportunity_name': f"{customer['customer_name']} - {product['product_name']}",
        'stage': stage,
        'amount': round(random.uniform(10000, 1000000), 2),
        'probability': random.randint(10, 100),
        'close_date': created + timedelta(days=random.randint(30, 180)),
        'created_date': created,
        'opportunity_type': random.choice(opp_types),
        'region': customer['region'],
        'sales_rep': customer['account_owner'],
        'lead_source': random.choice(['Marketing', 'Sales', 'Partner', 'Web', 'Referral']),
        'competitor': random.choice(['Competitor A', 'Competitor B', 'Competitor C', None, None])
    })

opportunities_df = pd.DataFrame(opportunities_data)
opportunities_df.to_csv(f"{OUTPUT_DIR}/sales_data/opportunities.csv", index=False)
print(f"✓ Generated {len(opportunities_df)} opportunities")

# 5. CLOSED_DEALS
print("Generating CLOSED_DEALS table...")
closed_deals_data = []
win_loss_reasons = {
    'Won': ['Best Price', 'Best Features', 'Relationship', 'Timing', 'Superior Support'],
    'Lost': ['Pricing', 'Competition', 'Features', 'Timing', 'Budget', 'No Decision']
}

closed_opps = opportunities_df[opportunities_df['stage'].isin(['Closed Won', 'Closed Lost'])]
deal_id = 1

for _, opp in closed_opps.iterrows():
    status = 'Won' if opp['stage'] == 'Closed Won' else 'Lost'
    close_date = opp['close_date']
    quarter = f"Q{((close_date.month-1)//3)+1} {close_date.year}"
    
    closed_deals_data.append({
        'deal_id': generate_id('DEAL', deal_id),
        'opportunity_id': opp['opportunity_id'],
        'customer_id': opp['customer_id'],
        'product_id': opp['product_id'],
        'deal_value': opp['amount'] if status == 'Won' else 0,
        'close_date': close_date,
        'quarter': quarter,
        'fiscal_year': close_date.year,
        'status': status,
        'win_loss_reason': random.choice(win_loss_reasons[status]),
        'sales_cycle_days': (close_date - opp['created_date']).days,
        'discount_percent': round(random.uniform(0, 25), 2) if status == 'Won' else 0,
        'region': opp['region'],
        'sales_rep': opp['sales_rep']
    })
    deal_id += 1

closed_deals_df = pd.DataFrame(closed_deals_data)
closed_deals_df.to_csv(f"{OUTPUT_DIR}/sales_data/closed_deals.csv", index=False)
print(f"✓ Generated {len(closed_deals_df)} closed deals")

# 6. SALES_TARGETS
print("Generating SALES_TARGETS table...")
sales_targets_data = []
target_id = 1

for year in [2023, 2024, 2025]:
    for q in range(1, 5):
        quarter = f"Q{q} {year}"
        for region in REGIONS:
            sales_targets_data.append({
                'target_id': generate_id('TGT', target_id),
                'quarter': quarter,
                'region': region,
                'product_id': None,
                'target_amount': round(random.uniform(5000000, 20000000), 2),
                'sales_rep': None
            })
            target_id += 1

sales_targets_df = pd.DataFrame(sales_targets_data)
sales_targets_df.to_csv(f"{OUTPUT_DIR}/sales_data/sales_targets.csv", index=False)
print(f"✓ Generated {len(sales_targets_df)} sales targets")

# 7. CUSTOMER_ATTRIBUTES
print("Generating CUSTOMER_ATTRIBUTES table...")
customer_attributes_data = []
attr_id = 1
attribute_types = ['Certification', 'Technology Stack', 'Use Case', 'Integration', 'Compliance']
attribute_values = {
    'Certification': ['ISO 9001', 'SOC 2', 'HIPAA', 'GDPR', 'PCI DSS'],
    'Technology Stack': ['AWS', 'Azure', 'GCP', 'On-Premise', 'Hybrid'],
    'Use Case': ['Analytics', 'Reporting', 'Real-time Processing', 'Data Warehousing', 'ML/AI'],
    'Integration': ['Salesforce', 'SAP', 'Oracle', 'Microsoft', 'Custom'],
    'Compliance': ['SOX', 'FINRA', 'FDA', 'CCPA', 'Privacy Shield']
}

for _, customer in customers_df.iterrows():
    # Each customer gets 2-5 attributes
    num_attrs = random.randint(2, 5)
    for _ in range(num_attrs):
        attr_type = random.choice(attribute_types)
        customer_attributes_data.append({
            'attribute_id': generate_id('ATTR', attr_id),
            'customer_id': customer['customer_id'],
            'attribute_type': attr_type,
            'attribute_value': random.choice(attribute_values[attr_type]),
            'created_date': random_date(customer['created_date'], END_DATE)
        })
        attr_id += 1

customer_attributes_df = pd.DataFrame(customer_attributes_data)
customer_attributes_df.to_csv(f"{OUTPUT_DIR}/sales_data/customer_attributes.csv", index=False)
print(f"✓ Generated {len(customer_attributes_df)} customer attributes")

# 29. CUSTOMER_CONTACTS
print("Generating CUSTOMER_CONTACTS table...")
customer_contacts_data = []
contact_id = 1
titles = ['CEO', 'CTO', 'VP Engineering', 'Director IT', 'Manager', 'Analyst', 'Administrator']
roles = ['Decision Maker', 'Influencer', 'Champion', 'User', 'Blocker']

for _, customer in customers_df.iterrows():
    # Each customer has 2-6 contacts
    num_contacts = random.randint(2, 6)
    for i in range(num_contacts):
        first_name = f"Contact{contact_id}_First"
        last_name = f"Contact{contact_id}_Last"
        customer_contacts_data.append({
            'contact_id': generate_id('CONT', contact_id),
            'customer_id': customer['customer_id'],
            'first_name': first_name,
            'last_name': last_name,
            'email': f"{first_name.lower()}.{last_name.lower()}@{customer['customer_name'].replace(' ', '').lower()}.com",
            'phone': f"+1-555-{random.randint(100,999)}-{random.randint(1000,9999)}",
            'title': random.choice(titles),
            'department': random.choice(['IT', 'Engineering', 'Operations', 'Finance', 'Executive']),
            'role_type': random.choice(roles),
            'is_primary': 'Yes' if i == 0 else 'No',
            'linkedin_url': f"https://linkedin.com/in/{first_name.lower()}{last_name.lower()}",
            'last_contact_date': random_date(customer['created_date'], END_DATE),
            'created_date': customer['created_date'],
            'status': random.choice(['Active', 'Active', 'Active', 'Inactive'])
        })
        contact_id += 1

customer_contacts_df = pd.DataFrame(customer_contacts_data)
customer_contacts_df.to_csv(f"{OUTPUT_DIR}/sales_data/customer_contacts.csv", index=False)
print(f"✓ Generated {len(customer_contacts_df)} customer contacts")

print("\n=== Generating Product Usage & Telemetry Data ===")

# 8. PRODUCT_USAGE_SUMMARY
print("Generating PRODUCT_USAGE_SUMMARY table...")
# Add usage_tier to customer_products based on product_id
customer_products_with_tier = customer_products_df.copy()

# Create a mapping from product_id to usage_tier
if 'usage_tier' in products_df.columns:
    tier_map = dict(zip(products_df['product_id'], products_df['usage_tier']))
    # Map usage_tier to customer_products
    customer_products_with_tier['usage_tier'] = customer_products_with_tier['product_id'].map(tier_map)
    # Fill any missing values with 'Medium'
    customer_products_with_tier['usage_tier'] = customer_products_with_tier['usage_tier'].fillna('Medium')
else:
    # If usage_tier doesn't exist, default all to 'Medium'
    customer_products_with_tier['usage_tier'] = 'Medium'
    print("   Warning: usage_tier not found in products, defaulting to 'Medium' for all")

def get_usage_by_tier(usage_tier, license_count):
    """Generate usage metrics based on product usage tier"""
    if usage_tier == 'Low':
        return {
            'active_users': random.randint(1, max(3, int(license_count * 0.2))),
            'total_sessions': random.randint(10, 500),
            'avg_session_duration_min': round(random.uniform(5, 30), 2),
            'feature_adoption_score': round(random.uniform(20, 50), 2),
            'data_volume_gb': round(random.uniform(0.1, 50), 2),
            'api_calls': random.randint(10, 5000),
            'login_count': random.randint(5, 200),
            'license_utilization_pct': random.uniform(10, 40)
        }
    elif usage_tier == 'High':
        return {
            'active_users': random.randint(max(5, int(license_count * 0.7)), license_count),
            'total_sessions': random.randint(2000, 10000),
            'avg_session_duration_min': round(random.uniform(60, 180), 2),
            'feature_adoption_score': round(random.uniform(70, 98), 2),
            'data_volume_gb': round(random.uniform(500, 5000), 2),
            'api_calls': random.randint(50000, 500000),
            'login_count': random.randint(1000, 5000),
            'license_utilization_pct': random.uniform(70, 95)
        }
    else:  # Medium
        return {
            'active_users': random.randint(max(3, int(license_count * 0.3)), max(5, int(license_count * 0.7))),
            'total_sessions': random.randint(500, 3000),
            'avg_session_duration_min': round(random.uniform(20, 90), 2),
            'feature_adoption_score': round(random.uniform(45, 75), 2),
            'data_volume_gb': round(random.uniform(50, 500), 2),
            'api_calls': random.randint(5000, 50000),
            'login_count': random.randint(200, 1500),
            'license_utilization_pct': random.uniform(40, 70)
        }

usage_summary_data = []
usage_id = 1

for _, install in customer_products_with_tier.iterrows():
    # Generate weekly usage records from install date to now
    current_date = install['install_date']
    usage_tier = install['usage_tier']
    license_count = install['license_count']
    
    while current_date <= END_DATE:
        usage_metrics = get_usage_by_tier(usage_tier, license_count)
        
        usage_summary_data.append({
            'usage_id': generate_id('USG', usage_id),
            'customer_id': install['customer_id'],
            'product_id': install['product_id'],
            'usage_date': current_date,
            'active_users': usage_metrics['active_users'],
            'total_sessions': usage_metrics['total_sessions'],
            'avg_session_duration_min': usage_metrics['avg_session_duration_min'],
            'feature_adoption_score': usage_metrics['feature_adoption_score'],
            'data_volume_gb': usage_metrics['data_volume_gb'],
            'api_calls': usage_metrics['api_calls'],
            'error_rate_percent': round(random.uniform(0, 5), 2),
            'login_count': usage_metrics['login_count'],
            'license_utilization': round(usage_metrics['license_utilization_pct'], 2),
            'health_score': random.choice(['Healthy', 'Healthy', 'Healthy', 'At Risk', 'Critical'])
        })
        usage_id += 1
        current_date += timedelta(days=7)  # Weekly records

usage_summary_df = pd.DataFrame(usage_summary_data)
usage_summary_df.to_csv(f"{OUTPUT_DIR}/usage_data/product_usage_summary.csv", index=False)
print(f"✓ Generated {len(usage_summary_df)} usage summary records")

# 9. FEATURE_USAGE
print("Generating FEATURE_USAGE table...")
feature_usage_data = []
feature_usage_id = 1
features = ['Dashboard', 'Reports', 'Export', 'API', 'Integration', 'Analytics', 'Search', 'Admin Panel']

for _, install in customer_products_with_tier.head(500).iterrows():  # Subset for performance
    usage_tier = install['usage_tier']
    
    # Adjust feature usage based on tier
    if usage_tier == 'Low':
        num_features = random.randint(2, 4)
        usage_range = (1, 100)
        users_range = (1, 15)
    elif usage_tier == 'High':
        num_features = random.randint(5, 8)
        usage_range = (200, 1000)
        users_range = (20, 100)
    else:  # Medium
        num_features = random.randint(3, 6)
        usage_range = (50, 300)
        users_range = (5, 40)
    
    for feature in random.sample(features, num_features):
        for _ in range(random.randint(5, 15)):
            feature_usage_data.append({
                'feature_usage_id': generate_id('FEAT', feature_usage_id),
                'customer_id': install['customer_id'],
                'product_id': install['product_id'],
                'feature_name': feature,
                'usage_date': random_date(install['install_date'], END_DATE),
                'usage_count': random.randint(*usage_range),
                'unique_users': random.randint(*users_range),
                'adoption_status': random.choice(['Not Adopted', 'Exploring', 'Regular', 'Power User'])
            })
            feature_usage_id += 1

feature_usage_df = pd.DataFrame(feature_usage_data)
feature_usage_df.to_csv(f"{OUTPUT_DIR}/usage_data/feature_usage.csv", index=False)
print(f"✓ Generated {len(feature_usage_df)} feature usage records")

print("\n=== Generating Marketing Data ===")

# 12. CAMPAIGNS
print("Generating CAMPAIGNS table...")
campaigns_data = []
campaign_types = ['Email', 'Webinar', 'Event', 'Social', 'Content', 'Partner']
channels = ['Email', 'LinkedIn', 'Trade Show', 'Webinar', 'Content Syndication']

for i in range(75):
    start = random_date(START_DATE, END_DATE - timedelta(days=60))
    campaigns_data.append({
        'campaign_id': generate_id('CAMP', i+1),
        'campaign_name': f"Campaign {i+1} - {random.choice(['Q1', 'Q2', 'Q3', 'Q4'])} {random.choice([2023, 2024, 2025])}",
        'campaign_type': random.choice(campaign_types),
        'channel': random.choice(channels),
        'start_date': start,
        'end_date': start + timedelta(days=random.randint(7, 90)),
        'budget': round(random.uniform(5000, 100000), 2),
        'target_audience': random.choice(['Enterprise', 'SMB', 'Mid-Market']),
        'region': random.choice(REGIONS),
        'product_focus': random.choice(list(products_df['product_id'])),
        'status': random.choice(['Planned', 'Active', 'Completed', 'Completed', 'Completed']),
        'owner': f"Marketing_Manager_{random.randint(1, 10)}"
    })

campaigns_df = pd.DataFrame(campaigns_data)
campaigns_df.to_csv(f"{OUTPUT_DIR}/marketing_data/campaigns.csv", index=False)
print(f"✓ Generated {len(campaigns_df)} campaigns")

# 14. LEADS
print("Generating LEADS table...")
leads_data = []
lead_statuses = ['New', 'Contacted', 'Qualified', 'Unqualified', 'Converted']

for i in range(7000):
    campaign = campaigns_df.sample(1).iloc[0] if random.random() > 0.3 else None
    created = random_date(START_DATE, END_DATE)
    status = random.choice(lead_statuses)
    
    leads_data.append({
        'lead_id': generate_id('LEAD', i+1),
        'company_name': f"Lead Company {i+1}",
        'contact_name': f"Lead Contact {i+1}",
        'email': f"lead{i+1}@company{i+1}.com",
        'phone': f"+1-555-{random.randint(100,999)}-{random.randint(1000,9999)}",
        'title': random.choice(titles),
        'industry': random.choice(INDUSTRIES),
        'company_size': random.choice(COMPANY_SIZES),
        'region': random.choice(REGIONS),
        'country': random.choice([c for countries in COUNTRIES.values() for c in countries]),
        'lead_source': campaign['campaign_type'] if campaign is not None else random.choice(['Website', 'Referral', 'Cold Call']),
        'campaign_id': campaign['campaign_id'] if campaign is not None else None,
        'event_id': None,
        'lead_score': random.randint(0, 100),
        'lead_status': status,
        'created_date': created,
        'last_activity_date': created + timedelta(days=random.randint(0, 30)),
        'assigned_to': f"Rep_{random.randint(1, 50)}",
        'products_interested': random.choice(list(products_df['product_name'])),
        'converted_to_opp_id': random.choice(list(opportunities_df['opportunity_id'])) if status == 'Converted' and random.random() > 0.5 else None,
        'conversion_date': created + timedelta(days=random.randint(7, 60)) if status == 'Converted' else None
    })

leads_df = pd.DataFrame(leads_data)
leads_df.to_csv(f"{OUTPUT_DIR}/marketing_data/leads.csv", index=False)
print(f"✓ Generated {len(leads_df)} leads")

print("\n=== Generating Support Data ===")

# 18. SUPPORT_TICKETS
print("Generating SUPPORT_TICKETS table...")
support_tickets_data = []
ticket_types = ['Bug', 'Feature Request', 'Question', 'Configuration', 'Performance', 'Security']
priorities = ['Low', 'Medium', 'High', 'Critical']
severities = ['Minor', 'Major', 'Critical', 'Blocker']
statuses = ['New', 'Open', 'In Progress', 'Pending Customer', 'Resolved', 'Closed']
categories = ['Technical', 'Billing', 'Access', 'Training', 'Integration', 'Other']

for i in range(10000):
    customer = customers_df.sample(1).iloc[0]
    # Get a product this customer has
    cust_products = customer_products_df[customer_products_df['customer_id'] == customer['customer_id']]
    if len(cust_products) > 0:
        product = cust_products.sample(1).iloc[0]
        product_id = product['product_id']
        version = product['version_number']
    else:
        product_id = products_df.sample(1).iloc[0]['product_id']
        version = products_df[products_df['product_id'] == product_id].iloc[0]['current_version']
    
    created = random_date(START_DATE, END_DATE)
    status = random.choice(statuses)
    priority = random.choice(priorities)
    
    ticket_data = {
        'ticket_id': generate_id('TICK', i+1),
        'ticket_number': f"TICK-{str(i+1).zfill(5)}",
        'customer_id': customer['customer_id'],
        'product_id': product_id,
        'contact_name': f"Contact at {customer['customer_name']}",
        'contact_email': f"contact{i+1}@{customer['customer_name'].replace(' ', '').lower()}.com",
        'subject': f"Issue with {random.choice(['login', 'performance', 'data sync', 'integration', 'reporting'])}",
        'description': f"Customer reported issue #{i+1}",
        'priority': priority,
        'severity': random.choice(severities),
        'status': status,
        'ticket_type': random.choice(ticket_types),
        'category': random.choice(categories),
        'created_date': created,
        'first_response_date': created + timedelta(hours=random.randint(1, 48)) if status != 'New' else None,
        'resolved_date': created + timedelta(days=random.randint(1, 30)) if status in ['Resolved', 'Closed'] else None,
        'closed_date': created + timedelta(days=random.randint(1, 35)) if status == 'Closed' else None,
        'assigned_to': f"Support_Engineer_{random.randint(1, 25)}",
        'escalated': 'Yes' if random.random() > 0.9 else 'No',
        'escalation_date': created + timedelta(days=random.randint(1, 5)) if random.random() > 0.9 else None,
        'sla_status': random.choice(['Met', 'Met', 'Met', 'Breached', 'At Risk']),
        'channel': random.choice(['Email', 'Phone', 'Chat', 'Portal', 'API']),
        'product_version': version,
        'environment': random.choice(['Production', 'Staging', 'Development'])
    }
    support_tickets_data.append(ticket_data)

support_tickets_df = pd.DataFrame(support_tickets_data)
support_tickets_df.to_csv(f"{OUTPUT_DIR}/support_data/support_tickets.csv", index=False)
print(f"✓ Generated {len(support_tickets_df)} support tickets")

# 21. CUSTOMER_SATISFACTION
print("Generating CUSTOMER_SATISFACTION table...")
csat_data = []
csat_id = 1

resolved_tickets = support_tickets_df[support_tickets_df['status'].isin(['Resolved', 'Closed'])]
for _, ticket in resolved_tickets.sample(n=min(5000, len(resolved_tickets))).iterrows():
    if random.random() > 0.4:  # 60% response rate
        csat_data.append({
            'csat_id': generate_id('CSAT', csat_id),
            'ticket_id': ticket['ticket_id'],
            'customer_id': ticket['customer_id'],
            'survey_date': ticket['closed_date'] if ticket['closed_date'] else ticket['resolved_date'],
            'response_date': (ticket['closed_date'] if ticket['closed_date'] else ticket['resolved_date']) + timedelta(days=random.randint(0, 3)),
            'satisfaction_score': random.randint(1, 5),
            'would_recommend': random.choice(['Yes', 'Yes', 'Yes', 'No']),
            'feedback_text': f"Feedback for ticket {ticket['ticket_number']}",
            'response_time_rating': random.randint(1, 5),
            'resolution_quality_rating': random.randint(1, 5),
            'agent_rating': random.randint(1, 5),
            'survey_type': 'Post-Ticket'
        })
        csat_id += 1

csat_df = pd.DataFrame(csat_data)
csat_df.to_csv(f"{OUTPUT_DIR}/support_data/customer_satisfaction.csv", index=False)
print(f"✓ Generated {len(csat_df)} CSAT records")

print("\n=== Generating Operational Data ===")

# 60. INVOICES
print("Generating INVOICES table...")
invoices_data = []
invoice_id = 1

for _, contract_prod in customer_products_df.iterrows():
    # Generate quarterly invoices
    invoice_date = contract_prod['install_date']
    while invoice_date <= END_DATE:
        invoices_data.append({
            'invoice_id': generate_id('INV', invoice_id),
            'invoice_number': f"INV-{str(invoice_id).zfill(6)}",
            'customer_id': contract_prod['customer_id'],
            'contract_id': None,
            'invoice_date': invoice_date,
            'due_date': invoice_date + timedelta(days=30),
            'total_amount': round(contract_prod['contract_value'] / 4, 2),
            'tax_amount': round((contract_prod['contract_value'] / 4) * 0.08, 2),
            'subtotal': round(contract_prod['contract_value'] / 4 * 0.92, 2),
            'status': random.choice(['Paid', 'Paid', 'Paid', 'Overdue', 'Sent']),
            'payment_method': random.choice(['Credit Card', 'ACH', 'Wire', 'Check']),
            'payment_date': invoice_date + timedelta(days=random.randint(5, 45)) if random.random() > 0.2 else None,
            'payment_reference': f"PAY-{random.randint(100000, 999999)}",
            'currency': 'USD',
            'billing_period_start': invoice_date,
            'billing_period_end': invoice_date + timedelta(days=90)
        })
        invoice_id += 1
        invoice_date += timedelta(days=90)  # Quarterly

invoices_df = pd.DataFrame(invoices_data)
invoices_df.to_csv(f"{OUTPUT_DIR}/operational_data/invoices.csv", index=False)
print(f"✓ Generated {len(invoices_df)} invoices")

# 53. CUSTOMER_HEALTH_SCORE
print("Generating CUSTOMER_HEALTH_SCORE table...")
health_score_data = []
health_id = 1

for _, customer in customers_df.iterrows():
    # Generate monthly health scores
    score_date = customer['created_date']
    while score_date <= END_DATE:
        product_usage_score = round(random.uniform(50, 100), 2)
        support_health_score = round(random.uniform(50, 100), 2)
        payment_health_score = round(random.uniform(70, 100), 2)
        engagement_score = round(random.uniform(40, 100), 2)
        sentiment_score = round(random.uniform(50, 100), 2)
        
        overall_score = round((product_usage_score + support_health_score + payment_health_score + engagement_score + sentiment_score) / 5, 2)
        
        if overall_score >= 80:
            risk_level = 'Low'
            churn_prob = round(random.uniform(0, 15), 2)
            expansion_prob = round(random.uniform(30, 70), 2)
        elif overall_score >= 60:
            risk_level = 'Medium'
            churn_prob = round(random.uniform(15, 35), 2)
            expansion_prob = round(random.uniform(10, 30), 2)
        elif overall_score >= 40:
            risk_level = 'High'
            churn_prob = round(random.uniform(35, 60), 2)
            expansion_prob = round(random.uniform(0, 10), 2)
        else:
            risk_level = 'Critical'
            churn_prob = round(random.uniform(60, 90), 2)
            expansion_prob = round(random.uniform(0, 5), 2)
        
        health_score_data.append({
            'health_score_id': generate_id('HLTH', health_id),
            'customer_id': customer['customer_id'],
            'score_date': score_date,
            'overall_score': overall_score,
            'product_usage_score': product_usage_score,
            'support_health_score': support_health_score,
            'payment_health_score': payment_health_score,
            'engagement_score': engagement_score,
            'sentiment_score': sentiment_score,
            'risk_level': risk_level,
            'churn_probability': churn_prob,
            'expansion_probability': expansion_prob,
            'health_trend': random.choice(['Improving', 'Stable', 'Stable', 'Declining'])
        })
        health_id += 1
        score_date += timedelta(days=30)  # Monthly

health_score_df = pd.DataFrame(health_score_data)
health_score_df.to_csv(f"{OUTPUT_DIR}/operational_data/customer_health_score.csv", index=False)
print(f"✓ Generated {len(health_score_df)} health score records")

print("\n" + "="*60)
print("DATA GENERATION COMPLETE!")
print("="*60)
print(f"\nSummary:")
print(f"  Customers: {len(customers_df)}")
print(f"  Products: {len(products_df)}")
print(f"  Customer Products: {len(customer_products_df)}")
print(f"  Opportunities: {len(opportunities_df)}")
print(f"  Closed Deals: {len(closed_deals_df)}")
print(f"  Sales Targets: {len(sales_targets_df)}")
print(f"  Customer Attributes: {len(customer_attributes_df)}")
print(f"  Customer Contacts: {len(customer_contacts_df)}")
print(f"  Product Usage: {len(usage_summary_df)}")
print(f"  Feature Usage: {len(feature_usage_df)}")
print(f"  Campaigns: {len(campaigns_df)}")
print(f"  Leads: {len(leads_df)}")
print(f"  Support Tickets: {len(support_tickets_df)}")
print(f"  CSAT Records: {len(csat_df)}")
print(f"  Invoices: {len(invoices_df)}")
print(f"  Health Scores: {len(health_score_df)}")
print(f"\nTotal records: ~{len(customers_df) + len(products_df) + len(customer_products_df) + len(opportunities_df) + len(closed_deals_df) + len(sales_targets_df) + len(customer_attributes_df) + len(customer_contacts_df) + len(usage_summary_df) + len(feature_usage_df) + len(campaigns_df) + len(leads_df) + len(support_tickets_df) + len(csat_df) + len(invoices_df) + len(health_score_df):,}")
print(f"\nFiles saved to: {OUTPUT_DIR}/")
print("\nNote: This is Part 1 of the generator (16 tables).")
print("Run the Part 2 and Part 3 scripts to generate all 62 tables.")
print("\nNext steps:")
print("1. Run Part 2 script for remaining tables")
print("2. Upload CSVs to S3 or cloud storage")
print("3. Load into Snowflake/Databricks using provided scripts")