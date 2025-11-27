"""
Sales Analytics Demo Dataset Generator - Part 4 (Missing Tables)
Generates the remaining 15 tables that were not in Parts 1-3
Run this after Parts 1, 2, and 3
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
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

# Load existing data from previous parts
print("Loading existing data from Parts 1-3...")
try:
    customers_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/customers.csv")
    products_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/products.csv")
    customer_products_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/customer_products.csv")
    contracts_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/contracts.csv")
    support_tickets_df = pd.read_csv(f"{OUTPUT_DIR}/support_data/support_tickets.csv")
    print("✓ Data loaded successfully!")
except Exception as e:
    print(f"Error loading data: {e}")
    print("Please ensure Parts 1, 2, and 3 have been run first.")
    exit(1)

def random_date(start, end):
    """Generate random date between start and end"""
    # Handle NaN values
    if pd.isna(start) or start is None:
        start = START_DATE
    if pd.isna(end) or end is None:
        end = END_DATE
    
    # Convert strings to datetime
    if isinstance(start, str):
        start = datetime.strptime(start, '%Y-%m-%d')
    if isinstance(end, str):
        end = datetime.strptime(end, '%Y-%m-%d')
    
    # Ensure we have datetime objects
    if not isinstance(start, datetime):
        start = START_DATE
    if not isinstance(end, datetime):
        end = END_DATE
    
    delta = end - start
    random_days = random.randint(0, max(1, delta.days))
    return start + timedelta(days=random_days)

def generate_id(prefix, num):
    """Generate ID with prefix"""
    return f"{prefix}-{str(num).zfill(6)}"

print("\n" + "="*80)
print("GENERATING MISSING TABLES - PART 4")
print("="*80)

# ============================================================================
# SALES_DATA SCHEMA - Missing Tables
# ============================================================================

print("\n=== Generating Missing SALES_DATA Tables ===")

# 36. REVENUE_RECOGNITION
print("Generating REVENUE_RECOGNITION table...")
revenue_recognition_data = []
recognition_id = 1

for _, contract in contracts_df.iterrows():
    if pd.isna(contract['start_date']):
        continue
    
    start_date = datetime.strptime(contract['start_date'], '%Y-%m-%d') if isinstance(contract['start_date'], str) else contract['start_date']
    end_date = datetime.strptime(contract['end_date'], '%Y-%m-%d') if isinstance(contract['end_date'], str) else contract['end_date']
    
    # Generate monthly revenue recognition entries
    current_date = start_date
    monthly_amount = contract['annual_value'] / 12 if pd.notna(contract['annual_value']) else 0
    total_recognized = 0
    
    while current_date <= end_date and current_date <= END_DATE:
        quarter = f"Q{((current_date.month-1)//3)+1} {current_date.year}"
        
        revenue_recognition_data.append({
            'recognition_id': generate_id('REVR', recognition_id),
            'contract_id': contract['contract_id'],
            'customer_id': contract['customer_id'],
            'recognition_date': current_date,
            'recognition_amount': round(monthly_amount, 2),
            'recognition_type': 'Monthly',
            'fiscal_quarter': quarter,
            'fiscal_year': current_date.year,
            'product_id': None,  # Could link to product if needed
            'region': None,  # Could get from customer if needed
            'deferred_revenue': round(contract['contract_value'] - total_recognized - monthly_amount, 2) if pd.notna(contract['contract_value']) else 0,
            'recognized_to_date': round(total_recognized + monthly_amount, 2)
        })
        
        total_recognized += monthly_amount
        recognition_id += 1
        current_date += timedelta(days=30)

revenue_recognition_df = pd.DataFrame(revenue_recognition_data)
revenue_recognition_df.to_csv(f"{OUTPUT_DIR}/sales_data/revenue_recognition.csv", index=False)
print(f"✓ Generated {len(revenue_recognition_df)} revenue recognition records")

# 37. TERRITORIES
print("Generating TERRITORIES table...")
territories_data = []
regions = ['North America', 'EMEA', 'APAC', 'Latin America']
territory_types = ['Geographic', 'Named Accounts', 'Industry']

territory_id = 1
for region in regions:
    # Create 3-5 territories per region
    num_territories = random.randint(3, 5)
    for i in range(num_territories):
        territories_data.append({
            'territory_id': generate_id('TERR', territory_id),
            'territory_name': f"{region} - Territory {i+1}",
            'region': region,
            'country_list': 'Multiple',
            'territory_type': random.choice(territory_types),
            'sales_rep': f"Rep_{random.randint(1, 50)}",
            'manager': f"Manager_{random.randint(1, 10)}",
            'quota': round(random.uniform(1000000, 5000000), 2),
            'active': random.choice(['Yes', 'Yes', 'Yes', 'No']),
            'effective_date': random_date(START_DATE, END_DATE)
        })
        territory_id += 1

territories_df = pd.DataFrame(territories_data)
territories_df.to_csv(f"{OUTPUT_DIR}/sales_data/territories.csv", index=False)
print(f"✓ Generated {len(territories_df)} territories")

# ============================================================================
# USAGE_DATA SCHEMA - Missing Tables
# ============================================================================

print("\n=== Generating Missing USAGE_DATA Tables ===")

# 42. DATA_QUALITY_METRICS
print("Generating DATA_QUALITY_METRICS table...")
data_quality_data = []
quality_id = 1

for _, install in customer_products_df.head(300).iterrows():
    current_date = datetime.strptime(install['install_date'], '%Y-%m-%d') if isinstance(install['install_date'], str) else install['install_date']
    
    # Generate monthly quality metrics
    while current_date <= END_DATE:
        completeness = round(random.uniform(70, 100), 2)
        accuracy = round(random.uniform(75, 100), 2)
        consistency = round(random.uniform(70, 100), 2)
        timeliness = round(random.uniform(80, 100), 2)
        
        overall = round((completeness + accuracy + consistency + timeliness) / 4, 2)
        
        data_quality_data.append({
            'quality_metric_id': generate_id('QUAL', quality_id),
            'customer_id': install['customer_id'],
            'product_id': install['product_id'],
            'metric_date': current_date,
            'completeness_score': completeness,
            'accuracy_score': accuracy,
            'consistency_score': consistency,
            'timeliness_score': timeliness,
            'duplicate_records': random.randint(0, 100),
            'null_values_percent': round(random.uniform(0, 10), 2),
            'validation_errors': random.randint(0, 50),
            'overall_quality_score': overall
        })
        quality_id += 1
        current_date += timedelta(days=30)

data_quality_df = pd.DataFrame(data_quality_data)
data_quality_df.to_csv(f"{OUTPUT_DIR}/usage_data/data_quality_metrics.csv", index=False)
print(f"✓ Generated {len(data_quality_df)} data quality metrics")

# 43. PERFORMANCE_METRICS
print("Generating PERFORMANCE_METRICS table...")
performance_data = []
perf_id = 1

for _, install in customer_products_df.head(300).iterrows():
    current_date = datetime.strptime(install['install_date'], '%Y-%m-%d') if isinstance(install['install_date'], str) else install['install_date']
    
    # Generate monthly performance metrics
    while current_date <= END_DATE:
        performance_data.append({
            'performance_id': generate_id('PERF', perf_id),
            'customer_id': install['customer_id'],
            'product_id': install['product_id'],
            'metric_date': current_date,
            'avg_page_load_time_ms': round(random.uniform(200, 2000), 2),
            'avg_query_time_ms': round(random.uniform(50, 500), 2),
            'uptime_percent': round(random.uniform(99.0, 100.0), 2),
            'downtime_minutes': random.randint(0, 30),
            'peak_concurrent_users': random.randint(50, 500),
            'avg_concurrent_users': random.randint(20, 200),
            'cpu_utilization_percent': round(random.uniform(30, 80), 2),
            'memory_utilization_percent': round(random.uniform(40, 85), 2),
            'storage_used_gb': round(random.uniform(100, 5000), 2)
        })
        perf_id += 1
        current_date += timedelta(days=30)

performance_df = pd.DataFrame(performance_data)
performance_df.to_csv(f"{OUTPUT_DIR}/usage_data/performance_metrics.csv", index=False)
print(f"✓ Generated {len(performance_df)} performance metrics")

# ============================================================================
# SUPPORT_DATA SCHEMA - Missing Tables
# ============================================================================

print("\n=== Generating Missing SUPPORT_DATA Tables ===")

# 24. KNOWLEDGE_BASE
print("Generating KNOWLEDGE_BASE table...")
kb_data = []
categories = ['Installation', 'Configuration', 'Troubleshooting', 'How-To', 'FAQ']
kb_titles = [
    'How to Install {}',
    'Configuring {} for Production',
    'Troubleshooting Common {} Issues',
    'Best Practices for {}',
    '{} Integration Guide',
    'Getting Started with {}',
    'Advanced {} Configuration',
    'Performance Tuning for {}',
    '{} Security Settings',
    'Migrating to Latest {} Version'
]

for i in range(250):
    product = products_df.sample(1).iloc[0]
    title_template = random.choice(kb_titles)
    
    kb_data.append({
        'kb_id': generate_id('KB', i+1),
        'article_title': title_template.format(product['product_name']),
        'article_category': random.choice(categories),
        'product_id': product['product_id'],
        'product_version': product['current_version'],
        'content': f"Knowledge base article content for {product['product_name']}...",
        'created_date': random_date(START_DATE, END_DATE),
        'last_updated': random_date(START_DATE, END_DATE),
        'author': f"Support_Engineer_{random.randint(1, 25)}",
        'views_count': random.randint(10, 10000),
        'helpfulness_score': round(random.uniform(3.0, 5.0), 2),
        'related_tickets_count': random.randint(0, 100),
        'status': random.choice(['Published', 'Published', 'Published', 'Draft', 'Archived']),
        'tags': ','.join(random.sample(['api', 'integration', 'error', 'config', 'security', 'performance'], random.randint(2, 4)))
    })

kb_df = pd.DataFrame(kb_data)
kb_df.to_csv(f"{OUTPUT_DIR}/support_data/knowledge_base.csv", index=False)
print(f"✓ Generated {len(kb_df)} knowledge base articles")

# 25. TICKET_TAGS
print("Generating TICKET_TAGS table...")
ticket_tags_data = []
tag_id = 1
tag_names = ['urgent', 'api-issue', 'data-loss', 'performance', 'security', 'integration', 
             'billing', 'bug', 'feature-request', 'documentation', 'training', 'migration']

for _, ticket in support_tickets_df.head(5000).iterrows():
    # Each ticket gets 1-3 tags
    num_tags = random.randint(1, 3)
    for tag in random.sample(tag_names, num_tags):
        ticket_tags_data.append({
            'tag_id': generate_id('TAG', tag_id),
            'ticket_id': ticket['ticket_id'],
            'tag_name': tag,
            'tagged_by': ticket['assigned_to'],
            'tagged_date': random_date(ticket['created_date'], 
                                      ticket['closed_date'] if pd.notna(ticket['closed_date']) else END_DATE)
        })
        tag_id += 1

ticket_tags_df = pd.DataFrame(ticket_tags_data)
ticket_tags_df.to_csv(f"{OUTPUT_DIR}/support_data/ticket_tags.csv", index=False)
print(f"✓ Generated {len(ticket_tags_df)} ticket tags")

# 26. ESCALATIONS
print("Generating ESCALATIONS table...")
escalations_data = []
escalation_levels = ['L1 → L2', 'L2 → L3', 'L3 → Engineering', 'Manager']
escalation_reasons = ['Complex Issue', 'SLA Risk', 'Customer Request', 'Severity', 'Technical Expertise Required']

escalated_tickets = support_tickets_df[support_tickets_df['escalated'] == 'Yes']
for i, (_, ticket) in enumerate(escalated_tickets.iterrows()):
    escalations_data.append({
        'escalation_id': generate_id('ESC', i+1),
        'ticket_id': ticket['ticket_id'],
        'customer_id': ticket['customer_id'],
        'escalation_level': random.choice(escalation_levels),
        'escalated_from': ticket['assigned_to'],
        'escalated_to': f"Support_Level_{random.randint(2, 4)}",
        'escalation_date': ticket['escalation_date'],
        'escalation_reason': random.choice(escalation_reasons),
        'resolved_at_level': random.choice(['L2', 'L3', 'Engineering']),
        'resolution_date': random_date(ticket['escalation_date'], 
                                      ticket['closed_date'] if pd.notna(ticket['closed_date']) else END_DATE)
    })

escalations_df = pd.DataFrame(escalations_data)
escalations_df.to_csv(f"{OUTPUT_DIR}/support_data/escalations.csv", index=False)
print(f"✓ Generated {len(escalations_df)} escalations")

# 27. RECURRING_ISSUES
print("Generating RECURRING_ISSUES table...")
recurring_issues_data = []
issue_patterns = [
    'Login fails with SSO integration',
    'API timeout errors during peak hours',
    'Data sync delays with external systems',
    'Report generation performance issues',
    'Export functionality crashes with large datasets',
    'Mobile app crashes on iOS 16',
    'Dashboard widgets not loading',
    'Email notifications not being sent',
    'Permission errors after version upgrade',
    'Search functionality returns incomplete results'
]

for i, pattern in enumerate(issue_patterns):
    for version in random.sample(list(products_df['current_version']), random.randint(1, 3)):
        product = products_df[products_df['current_version'] == version].sample(1).iloc[0]
        first_occurrence = random_date(START_DATE, END_DATE - timedelta(days=60))
        
        recurring_issues_data.append({
            'recurring_issue_id': generate_id('RECUR', len(recurring_issues_data)+1),
            'issue_pattern': pattern,
            'product_id': product['product_id'],
            'product_version': version,
            'first_occurrence': first_occurrence,
            'last_occurrence': random_date(first_occurrence, END_DATE),
            'occurrence_count': random.randint(5, 100),
            'affected_customers_count': random.randint(3, 50),
            'priority': random.choice(['Low', 'Medium', 'High', 'Critical']),
            'status': random.choice(['Open', 'Under Investigation', 'Fix in Progress', 'Resolved', 'Resolved']),
            'assigned_team': random.choice(['Engineering', 'Product', 'DevOps']),
            'related_tickets': ','.join([f"TICK-{random.randint(1, 10000):05d}" for _ in range(random.randint(3, 10))]),
            'root_cause': random.choice(['Product Bug', 'Infrastructure', 'Configuration', 'Third-party Integration']) if random.random() > 0.3 else None,
            'fix_version': f"{random.randint(2,5)}.{random.randint(0,9)}.{random.randint(0,20)}" if random.random() > 0.4 else None
        })

recurring_issues_df = pd.DataFrame(recurring_issues_data)
recurring_issues_df.to_csv(f"{OUTPUT_DIR}/support_data/recurring_issues.csv", index=False)
print(f"✓ Generated {len(recurring_issues_df)} recurring issues")

# 28. SUPPORT_AGENT_METRICS
print("Generating SUPPORT_AGENT_METRICS table...")
agent_metrics_data = []
metric_id = 1
agents = [f"Support_Engineer_{i}" for i in range(1, 26)]

# Generate weekly metrics for each agent
current_date = START_DATE
while current_date <= END_DATE:
    for agent in agents:
        tickets_opened = random.randint(10, 50)
        tickets_closed = int(tickets_opened * random.uniform(0.7, 1.1))
        
        agent_metrics_data.append({
            'metric_id': generate_id('AGNTM', metric_id),
            'agent_name': agent,
            'metric_date': current_date,
            'tickets_opened': tickets_opened,
            'tickets_closed': tickets_closed,
            'avg_resolution_time_hrs': round(random.uniform(4, 48), 2),
            'first_response_time_avg': round(random.uniform(0.5, 8), 2),
            'csat_score_avg': round(random.uniform(3.5, 5.0), 2),
            'sla_breach_count': random.randint(0, 5),
            'escalation_count': random.randint(0, 10),
            'reopened_tickets_count': random.randint(0, 5),
            'utilization_percent': round(random.uniform(70, 95), 2)
        })
        metric_id += 1
    current_date += timedelta(days=7)

agent_metrics_df = pd.DataFrame(agent_metrics_data)
agent_metrics_df.to_csv(f"{OUTPUT_DIR}/support_data/support_agent_metrics.csv", index=False)
print(f"✓ Generated {len(agent_metrics_df)} agent metrics records")

# 51. SUPPORT_CHANNELS
print("Generating SUPPORT_CHANNELS table...")
channels_data = [
    {
        'channel_id': generate_id('CHAN', 1),
        'channel_name': 'Email',
        'active': 'Yes',
        'hours_of_operation': '24/7',
        'avg_response_time_min': round(random.uniform(30, 120), 2),
        'satisfaction_score': round(random.uniform(4.0, 4.8), 2),
        'volume_percent': 45.0,
        'cost_per_ticket': round(random.uniform(10, 25), 2)
    },
    {
        'channel_id': generate_id('CHAN', 2),
        'channel_name': 'Phone',
        'active': 'Yes',
        'hours_of_operation': 'Business Hours',
        'avg_response_time_min': round(random.uniform(2, 10), 2),
        'satisfaction_score': round(random.uniform(4.2, 4.9), 2),
        'volume_percent': 25.0,
        'cost_per_ticket': round(random.uniform(25, 50), 2)
    },
    {
        'channel_id': generate_id('CHAN', 3),
        'channel_name': 'Chat',
        'active': 'Yes',
        'hours_of_operation': '24/7',
        'avg_response_time_min': round(random.uniform(1, 5), 2),
        'satisfaction_score': round(random.uniform(4.3, 4.9), 2),
        'volume_percent': 20.0,
        'cost_per_ticket': round(random.uniform(8, 15), 2)
    },
    {
        'channel_id': generate_id('CHAN', 4),
        'channel_name': 'Portal',
        'active': 'Yes',
        'hours_of_operation': '24/7',
        'avg_response_time_min': round(random.uniform(60, 180), 2),
        'satisfaction_score': round(random.uniform(3.8, 4.5), 2),
        'volume_percent': 8.0,
        'cost_per_ticket': round(random.uniform(5, 12), 2)
    },
    {
        'channel_id': generate_id('CHAN', 5),
        'channel_name': 'Social Media',
        'active': 'Yes',
        'hours_of_operation': 'Business Hours',
        'avg_response_time_min': round(random.uniform(15, 60), 2),
        'satisfaction_score': round(random.uniform(3.5, 4.2), 2),
        'volume_percent': 2.0,
        'cost_per_ticket': round(random.uniform(12, 20), 2)
    }
]

channels_df = pd.DataFrame(channels_data)
channels_df.to_csv(f"{OUTPUT_DIR}/support_data/support_channels.csv", index=False)
print(f"✓ Generated {len(channels_df)} support channels")

# 52. TICKET_WORKLOG
print("Generating TICKET_WORKLOG table...")
worklog_data = []
worklog_id = 1

for _, ticket in support_tickets_df.head(3000).iterrows():
    # Each ticket gets 2-6 worklog entries
    num_entries = random.randint(2, 6)
    current_date = datetime.strptime(ticket['created_date'], '%Y-%m-%d') if isinstance(ticket['created_date'], str) else ticket['created_date']
    
    for i in range(num_entries):
        work_date = current_date + timedelta(days=random.randint(0, 5))
        
        worklog_data.append({
            'worklog_id': generate_id('WORK', worklog_id),
            'ticket_id': ticket['ticket_id'],
            'agent_name': ticket['assigned_to'],
            'work_date': work_date,
            'time_spent_minutes': random.randint(15, 240),
            'work_description': f"Work performed on {ticket['ticket_number']}",
            'billable': random.choice(['Yes', 'Yes', 'No']),
            'internal_only': random.choice(['Yes', 'No', 'No'])
        })
        worklog_id += 1

worklog_df = pd.DataFrame(worklog_data)
worklog_df.to_csv(f"{OUTPUT_DIR}/support_data/ticket_worklog.csv", index=False)
print(f"✓ Generated {len(worklog_df)} worklog entries")

# ============================================================================
# OPERATIONAL_DATA SCHEMA - Missing Tables
# ============================================================================

print("\n=== Generating Missing OPERATIONAL_DATA Tables ===")

# 54. ONBOARDING_PROGRESS
print("Generating ONBOARDING_PROGRESS table...")
onboarding_data = []

for i, (_, install) in enumerate(customer_products_df.head(500).iterrows()):
    start_date = datetime.strptime(install['install_date'], '%Y-%m-%d') if isinstance(install['install_date'], str) else install['install_date']
    target_days = random.randint(30, 90)
    target_date = start_date + timedelta(days=target_days)
    
    status = random.choice(['Not Started', 'In Progress', 'In Progress', 'In Progress', 'Completed', 'Completed', 'Stalled'])
    
    if status == 'Completed':
        actual_date = start_date + timedelta(days=random.randint(target_days - 20, target_days + 30))
        progress = 100
        milestones_completed = 8
    elif status == 'In Progress':
        actual_date = None
        progress = round(random.uniform(30, 85), 2)
        milestones_completed = int(8 * progress / 100)
    else:
        actual_date = None
        progress = round(random.uniform(0, 25), 2)
        milestones_completed = int(8 * progress / 100)
    
    onboarding_data.append({
        'onboarding_id': generate_id('ONBRD', i+1),
        'customer_id': install['customer_id'],
        'product_id': install['product_id'],
        'start_date': start_date,
        'target_completion_date': target_date,
        'actual_completion_date': actual_date,
        'status': status,
        'progress_percent': progress,
        'milestone_count': 8,
        'milestones_completed': milestones_completed,
        'csm_assigned': f"CSM_{random.randint(1, 15)}",
        'health_status': random.choice(['Green', 'Yellow', 'Red']) if status != 'Completed' else 'Green',
        'days_to_first_value': random.randint(7, 45) if status == 'Completed' else None,
        'adoption_rate': round(random.uniform(40, 95), 2) if status == 'Completed' else round(random.uniform(10, 60), 2)
    })

onboarding_df = pd.DataFrame(onboarding_data)
onboarding_df.to_csv(f"{OUTPUT_DIR}/operational_data/onboarding_progress.csv", index=False)
print(f"✓ Generated {len(onboarding_df)} onboarding records")

# 55. PRODUCT_RELEASES
print("Generating PRODUCT_RELEASES table...")
releases_data = []
release_id = 1

for _, product in products_df.iterrows():
    # Generate 5-8 releases per product
    num_releases = random.randint(5, 8)
    current_version = [2, 0, 0]
    
    for i in range(num_releases):
        release_date = random_date(START_DATE, END_DATE)
        release_type = random.choice(['Major', 'Minor', 'Minor', 'Minor', 'Patch', 'Patch'])
        
        if release_type == 'Major':
            current_version[0] += 1
            current_version[1] = 0
            current_version[2] = 0
        elif release_type == 'Minor':
            current_version[1] += 1
            current_version[2] = 0
        else:
            current_version[2] += 1
        
        version_str = f"{current_version[0]}.{current_version[1]}.{current_version[2]}"
        
        releases_data.append({
            'release_id': generate_id('REL', release_id),
            'product_id': product['product_id'],
            'version_number': version_str,
            'release_date': release_date,
            'release_type': release_type,
            'features_added': random.randint(5, 50) if release_type in ['Major', 'Minor'] else random.randint(0, 5),
            'bugs_fixed': random.randint(10, 100),
            'breaking_changes': 'Yes' if release_type == 'Major' and random.random() > 0.5 else 'No',
            'eol_date': release_date + timedelta(days=365*3),
            'support_end_date': release_date + timedelta(days=365*2),
            'adoption_rate_percent': round(random.uniform(10, 85), 2),
            'release_notes_url': f"https://docs.example.com/releases/{version_str}"
        })
        release_id += 1

releases_df = pd.DataFrame(releases_data)
releases_df.to_csv(f"{OUTPUT_DIR}/operational_data/product_releases.csv", index=False)
print(f"✓ Generated {len(releases_df)} product releases")

# 56. CUSTOMER_SUCCESS_ACTIVITIES
print("Generating CUSTOMER_SUCCESS_ACTIVITIES table...")
cs_activities_data = []
activity_types = ['QBR', 'Health Check', 'Training', 'Check-in', 'Escalation']
sentiments = ['Positive', 'Positive', 'Positive', 'Neutral', 'Negative']

for i in range(5000):
    customer = customers_df.sample(1).iloc[0]
    activity_date = random_date(customer['created_date'], END_DATE)
    
    cs_activities_data.append({
        'activity_id': generate_id('CSACT', i+1),
        'customer_id': customer['customer_id'],
        'activity_date': activity_date,
        'activity_type': random.choice(activity_types),
        'csm_name': f"CSM_{random.randint(1, 15)}",
        'outcome': f"Discussed product usage, upcoming renewals, and expansion opportunities.",
        'sentiment': random.choice(sentiments),
        'action_items': f"Follow up on feature requests, schedule training session.",
        'next_activity_date': activity_date + timedelta(days=random.randint(30, 90)),
        'attendees': f"{random.randint(2, 5)} attendees",
        'duration_minutes': random.randint(30, 90)
    })

cs_activities_df = pd.DataFrame(cs_activities_data)
cs_activities_df.to_csv(f"{OUTPUT_DIR}/operational_data/customer_success_activities.csv", index=False)
print(f"✓ Generated {len(cs_activities_df)} customer success activities")

# 57. USER_ACCOUNTS
print("Generating USER_ACCOUNTS table...")
user_accounts_data = []
roles = ['Admin', 'User', 'Viewer', 'Power User', 'Manager']
statuses = ['Active', 'Active', 'Active', 'Active', 'Inactive', 'Locked']
user_id = 1

for _, customer in customers_df.iterrows():
    # Each customer has 3-15 users
    num_users = random.randint(3, 15)
    
    for i in range(num_users):
        created = random_date(customer['created_date'], END_DATE)
        last_login = random_date(created, END_DATE) if random.random() > 0.2 else None
        status = random.choice(statuses)
        
        user_accounts_data.append({
            'user_account_id': generate_id('USER', user_id),
            'customer_id': customer['customer_id'],
            'username': f"user{user_id}@{customer['customer_name'].replace(' ', '').lower()}.com",
            'email': f"user{user_id}@{customer['customer_name'].replace(' ', '').lower()}.com",
            'first_name': f"FirstName{user_id}",
            'last_name': f"LastName{user_id}",
            'role': random.choice(roles),
            'status': status,
            'created_date': created,
            'last_login_date': last_login,
            'login_count': random.randint(0, 500) if status == 'Active' else random.randint(0, 50),
            'password_reset_date': random_date(created, END_DATE) if random.random() > 0.5 else None,
            'mfa_enabled': random.choice(['Yes', 'Yes', 'Yes', 'No'])
        })
        user_id += 1

user_accounts_df = pd.DataFrame(user_accounts_data)
user_accounts_df.to_csv(f"{OUTPUT_DIR}/operational_data/user_accounts.csv", index=False)
print(f"✓ Generated {len(user_accounts_df)} user accounts")

# 58. AUDIT_LOG
print("Generating AUDIT_LOG table...")
audit_log_data = []
event_types = ['Login', 'Logout', 'Create', 'Update', 'Delete', 'Export', 'Config_Change']
entity_types = ['Customer', 'Product', 'Ticket', 'User', 'Report', 'Dashboard', 'Integration']

for i in range(20000):
    customer = customers_df.sample(1).iloc[0]
    event_time = random_date(customer['created_date'], END_DATE)
    event_type = random.choice(event_types)
    success = random.choice([True, True, True, True, False])
    
    audit_log_data.append({
        'audit_id': generate_id('AUDIT', i+1),
        'customer_id': customer['customer_id'],
        'user_account_id': f"USER-{random.randint(1, user_id):06d}",
        'event_timestamp': event_time,
        'event_type': event_type,
        'entity_type': random.choice(entity_types),
        'entity_id': f"ENT-{random.randint(1, 10000):06d}",
        'action_description': f"{event_type} performed on {random.choice(entity_types)}",
        'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        'success': 'Yes' if success else 'No',
        'error_message': None if success else random.choice(['Access Denied', 'Invalid Parameters', 'Timeout', 'Not Found'])
    })

audit_log_df = pd.DataFrame(audit_log_data)
audit_log_df.to_csv(f"{OUTPUT_DIR}/operational_data/audit_log.csv", index=False)
print(f"✓ Generated {len(audit_log_df)} audit log entries")

# 59. NOTIFICATIONS
print("Generating NOTIFICATIONS table...")
notifications_data = []
notification_types = ['Alert', 'Reminder', 'Update', 'Promotion', 'System']
priorities = ['Low', 'Medium', 'High', 'Critical']
channels = ['Email', 'In-App', 'SMS', 'Push']
statuses = ['Sent', 'Delivered', 'Read', 'Failed']

for i in range(15000):
    customer = customers_df.sample(1).iloc[0]
    sent_date = random_date(customer['created_date'], END_DATE)
    status = random.choice(statuses)
    
    notifications_data.append({
        'notification_id': generate_id('NOTIF', i+1),
        'customer_id': customer['customer_id'],
        'user_account_id': f"USER-{random.randint(1, user_id):06d}" if random.random() > 0.3 else None,
        'notification_type': random.choice(notification_types),
        'priority': random.choice(priorities),
        'subject': f"Notification {i+1}: Important Update",
        'message': f"This is an important notification about your account.",
        'sent_date': sent_date,
        'read_date': sent_date + timedelta(hours=random.randint(1, 72)) if status == 'Read' else None,
        'status': status,
        'channel': random.choice(channels),
        'related_entity_type': random.choice(entity_types + [None, None]),
        'related_entity_id': f"ENT-{random.randint(1, 10000):06d}" if random.random() > 0.5 else None
    })

notifications_df = pd.DataFrame(notifications_data)
notifications_df.to_csv(f"{OUTPUT_DIR}/operational_data/notifications.csv", index=False)
print(f"✓ Generated {len(notifications_df)} notifications")

# 61. PAYMENTS
print("Generating PAYMENTS table...")
payments_data = []
payment_methods = ['Credit Card', 'ACH', 'Wire', 'Check']
payment_statuses = ['Successful', 'Successful', 'Successful', 'Failed', 'Pending', 'Refunded']
gateways = ['Stripe', 'PayPal', 'Square', 'Authorize.net']

# We'll create payments for a subset of customers
for i in range(8000):
    customer = customers_df.sample(1).iloc[0]
    payment_date = random_date(customer['created_date'], END_DATE)
    amount = round(random.uniform(1000, 50000), 2)
    status = random.choice(payment_statuses)
    method = random.choice(payment_methods)
    
    processing_fee = round(amount * 0.029, 2) if method == 'Credit Card' else round(amount * 0.008, 2)
    
    payments_data.append({
        'payment_id': generate_id('PAY', i+1),
        'invoice_id': f"INV-{random.randint(1, 10000):06d}",
        'customer_id': customer['customer_id'],
        'payment_date': payment_date,
        'payment_amount': amount,
        'payment_method': method,
        'transaction_id': f"TXN-{random.randint(100000, 999999)}",
        'status': status,
        'currency': 'USD',
        'processing_fee': processing_fee if status == 'Successful' else 0,
        'net_amount': amount - processing_fee if status == 'Successful' else 0,
        'payment_gateway': random.choice(gateways)
    })

payments_df = pd.DataFrame(payments_data)
payments_df.to_csv(f"{OUTPUT_DIR}/operational_data/payments.csv", index=False)
print(f"✓ Generated {len(payments_df)} payments")

# 62. RENEWALS
print("Generating RENEWALS table...")
renewals_data = []
renewal_statuses = ['Pending', 'Won', 'Lost', 'At Risk']
churn_reasons = ['Price', 'Product Fit', 'Competitor', 'Budget', 'Consolidation', 'Business Closure']

for i, (_, install) in enumerate(customer_products_df.iterrows()):
    if pd.isna(install['renewal_date']):
        continue
    
    renewal_date = datetime.strptime(install['renewal_date'], '%Y-%m-%d') if isinstance(install['renewal_date'], str) else install['renewal_date']
    current_exp = datetime.strptime(install['install_date'], '%Y-%m-%d') if isinstance(install['install_date'], str) else install['install_date']
    current_exp += timedelta(days=365)
    
    # Only create renewals for dates that have passed or are upcoming
    if renewal_date <= END_DATE + timedelta(days=90):
        status = random.choice(renewal_statuses)
        probability = random.randint(10, 100)
        
        if renewal_date < END_DATE - timedelta(days=30):
            status = random.choice(['Won', 'Won', 'Won', 'Lost'])
            probability = 100 if status == 'Won' else 0
        
        renewals_data.append({
            'renewal_id': generate_id('REN', i+1),
            'contract_id': None,  # Could link if contracts exist
            'customer_id': install['customer_id'],
            'current_expiration_date': current_exp,
            'renewal_date': renewal_date,
            'renewal_amount': round(install['contract_value'] * random.uniform(0.95, 1.15), 2),
            'renewal_type': random.choice(['Auto', 'Manual', 'Negotiated']),
            'renewal_status': status,
            'probability_percent': probability,
            'owner': f"Rep_{random.randint(1, 50)}",
            'last_contact_date': random_date(renewal_date - timedelta(days=90), renewal_date) if status != 'Pending' else None,
            'next_contact_date': renewal_date - timedelta(days=random.randint(7, 30)) if status == 'Pending' else None,
            'churn_risk': 'Low' if probability > 80 else 'Medium' if probability > 50 else 'High',
            'churn_reason': random.choice(churn_reasons) if status == 'Lost' else None,
            'notes': f"Renewal for {install['customer_id']}"
        })

renewals_df = pd.DataFrame(renewals_data)
renewals_df.to_csv(f"{OUTPUT_DIR}/operational_data/renewals.csv", index=False)
print(f"✓ Generated {len(renewals_df)} renewals")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("PART 4 GENERATION COMPLETE!")
print("="*80)

print(f"\nGenerated Tables Summary:")
print(f"\nSALES_DATA Schema:")
print(f"  ✓ revenue_recognition: {len(revenue_recognition_df):,} records")
print(f"  ✓ territories: {len(territories_df):,} records")

print(f"\nUSAGE_DATA Schema:")
print(f"  ✓ data_quality_metrics: {len(data_quality_df):,} records")
print(f"  ✓ performance_metrics: {len(performance_df):,} records")

print(f"\nSUPPORT_DATA Schema:")
print(f"  ✓ knowledge_base: {len(kb_df):,} records")
print(f"  ✓ ticket_tags: {len(ticket_tags_df):,} records")
print(f"  ✓ escalations: {len(escalations_df):,} records")
print(f"  ✓ recurring_issues: {len(recurring_issues_df):,} records")
print(f"  ✓ support_agent_metrics: {len(agent_metrics_df):,} records")
print(f"  ✓ support_channels: {len(channels_df):,} records")
print(f"  ✓ ticket_worklog: {len(worklog_df):,} records")

print(f"\nOPERATIONAL_DATA Schema:")
print(f"  ✓ onboarding_progress: {len(onboarding_df):,} records")
print(f"  ✓ product_releases: {len(releases_df):,} records")
print(f"  ✓ customer_success_activities: {len(cs_activities_df):,} records")
print(f"  ✓ user_accounts: {len(user_accounts_df):,} records")
print(f"  ✓ audit_log: {len(audit_log_df):,} records")
print(f"  ✓ notifications: {len(notifications_df):,} records")
print(f"  ✓ payments: {len(payments_df):,} records")
print(f"  ✓ renewals: {len(renewals_df):,} records")

total_new_records = (
    len(revenue_recognition_df) + len(territories_df) +
    len(data_quality_df) + len(performance_df) +
    len(kb_df) + len(ticket_tags_df) + len(escalations_df) + 
    len(recurring_issues_df) + len(agent_metrics_df) + len(channels_df) + len(worklog_df) +
    len(onboarding_df) + len(releases_df) + len(cs_activities_df) + 
    len(user_accounts_df) + len(audit_log_df) + len(notifications_df) + 
    len(payments_df) + len(renewals_df)
)

print(f"\nTotal new records in Part 4: {total_new_records:,}")
print(f"\n✅ All 62 tables are now complete!")
print(f"\nNext steps:")
print("1. Verify all CSV files are present in {OUTPUT_DIR}/")
print("2. Upload to S3 or cloud storage")
print("3. Load into Snowflake/Databricks using provided scripts")
print("\nFiles saved to:")
print(f"  - {OUTPUT_DIR}/sales_data/")
print(f"  - {OUTPUT_DIR}/usage_data/")
print(f"  - {OUTPUT_DIR}/support_data/")
print(f"  - {OUTPUT_DIR}/operational_data/") 