"""
Sales Analytics Demo Dataset Generator - Part 3 (Final)
Generates remaining tables: Marketing (36-43, 44-50), Support (10-11, 19-20, 22-28, 51-52, 54-56), Operational (57-59, 61-62)
Run this after Part 1 and Part 2
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import os

# Set random seed
np.random.seed(42)
random.seed(42)

# Get project root directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "sales_analytics_data")
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 9, 30)

# Load existing data
print("Loading existing data...")
customers_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/customers.csv")
products_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/products.csv")
campaigns_df = pd.read_csv(f"{OUTPUT_DIR}/marketing_data/campaigns.csv")
leads_df = pd.read_csv(f"{OUTPUT_DIR}/marketing_data/leads.csv")
opportunities_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/opportunities.csv")
support_tickets_df = pd.read_csv(f"{OUTPUT_DIR}/support_data/support_tickets.csv")
closed_deals_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/closed_deals.csv")
customer_products_df = pd.read_csv(f"{OUTPUT_DIR}/sales_data/customer_products.csv")

def random_date(start, end):
    if isinstance(start, str):
        start = datetime.strptime(start, '%Y-%m-%d')
    if isinstance(end, str):
        end = datetime.strptime(end, '%Y-%m-%d')
    delta = end - start
    random_days = random.randint(0, max(1, delta.days))
    return start + timedelta(days=random_days)

def generate_id(prefix, num):
    return f"{prefix}-{str(num).zfill(6)}"

print("\n=== Generating Additional Marketing Tables ===")

# 13. EVENTS
print("Generating EVENTS table...")
events_data = []
event_types = ['Conference', 'Webinar', 'Workshop', 'Trade Show', 'User Group']
cities = ['New York', 'San Francisco', 'London', 'Berlin', 'Tokyo', 'Singapore', 'Virtual']

for i in range(45):
    event_date = random_date(START_DATE, END_DATE)
    expected = random.randint(50, 1000)
    actual = int(expected * random.uniform(0.6, 1.2))
    
    events_data.append({
        'event_id': generate_id('EVT', i+1),
        'event_name': f"{random.choice(['Annual', 'Spring', 'Fall', 'Tech'])} {random.choice(event_types)} {2023+i//15}",
        'event_type': random.choice(event_types),
        'event_date': event_date,
        'location': random.choice(cities),
        'region': random.choice(['North America', 'EMEA', 'APAC', 'Latin America']),
        'expected_attendees': expected,
        'actual_attendees': actual,
        'budget': round(random.uniform(10000, 200000), 2),
        'products_showcased': ','.join(random.sample(list(products_df['product_name']), random.randint(1, 4))),
        'lead_goal': random.randint(20, 200),
        'campaign_id': random.choice(list(campaigns_df['campaign_id'])) if random.random() > 0.3 else None
    })

events_df = pd.DataFrame(events_data)
events_df.to_csv(f"{OUTPUT_DIR}/marketing_data/events.csv", index=False)
print(f"✓ Generated {len(events_df)} events")

# 15. CAMPAIGN_ENGAGEMENT
print("Generating CAMPAIGN_ENGAGEMENT table...")
engagement_data = []
engagement_types = ['Email Opened', 'Link Clicked', 'Form Filled', 'Downloaded', 'Attended', 'Registered']

for i in range(40000):
    campaign = campaigns_df.sample(1).iloc[0]
    lead = leads_df.sample(1).iloc[0]
    # Convert campaign end_date to datetime if it's a string
    campaign_end_date = campaign['end_date']
    if isinstance(campaign_end_date, str):
        campaign_end_date = datetime.strptime(campaign_end_date, '%Y-%m-%d')
    
    engagement_date = random_date(campaign['start_date'], min(campaign_end_date, END_DATE))
    
    engagement_data.append({
        'engagement_id': generate_id('ENG', i+1),
        'campaign_id': campaign['campaign_id'],
        'lead_id': lead['lead_id'],
        'contact_id': lead['email'],
        'engagement_type': random.choice(engagement_types),
        'engagement_date': engagement_date,
        'asset_name': f"Asset for {campaign['campaign_name']}",
        'engagement_score': random.randint(1, 10)
    })

engagement_df = pd.DataFrame(engagement_data)
engagement_df.to_csv(f"{OUTPUT_DIR}/marketing_data/campaign_engagement.csv", index=False)
print(f"✓ Generated {len(engagement_df)} campaign engagement records")

# 16. MARKETING_ATTRIBUTION
print("Generating MARKETING_ATTRIBUTION table...")
attribution_data = []
attribution_id = 1

# For opportunities that came from leads
for _, opp in opportunities_df[opportunities_df['lead_source'] == 'Marketing'].head(2000).iterrows():
    # Create 1-5 touchpoints
    num_touchpoints = random.randint(1, 5)
    for touchpoint in range(num_touchpoints):
        campaign = campaigns_df.sample(1).iloc[0]
        attribution_data.append({
            'attribution_id': generate_id('ATTRIB', attribution_id),
            'opportunity_id': opp['opportunity_id'],
            'deal_id': None,
            'campaign_id': campaign['campaign_id'],
            'event_id': random.choice(list(events_df['event_id'])) if random.random() > 0.7 else None,
            'touchpoint_order': touchpoint + 1,
            'touchpoint_date': random_date(opp['created_date'], opp['close_date']),
            'attribution_weight': round(1.0 / num_touchpoints, 2),
            'attribution_model': random.choice(['First Touch', 'Last Touch', 'Multi-Touch', 'Linear'])
        })
        attribution_id += 1

attribution_df = pd.DataFrame(attribution_data)
attribution_df.to_csv(f"{OUTPUT_DIR}/marketing_data/marketing_attribution.csv", index=False)
print(f"✓ Generated {len(attribution_df)} attribution records")

# 17. WEBINAR_ATTENDANCE
print("Generating WEBINAR_ATTENDANCE table...")
webinar_attendance_data = []
attendance_id = 1

webinar_events = events_df[events_df['event_type'] == 'Webinar']
if len(webinar_events) == 0:
    print("   Warning: No webinar events found, creating sample webinar attendance...")
    # Create some sample webinar attendance even if no webinar events exist
    for i in range(100):
        webinar_attendance_data.append({
            'attendance_id': generate_id('ATTEND', attendance_id),
            'event_id': events_df.sample(1).iloc[0]['event_id'] if len(events_df) > 0 else generate_id('EVT', 1),
            'lead_id': leads_df.sample(1).iloc[0]['lead_id'] if len(leads_df) > 0 else generate_id('LEAD', 1),
            'registration_date': random_date(START_DATE, END_DATE),
            'attended': random.choice(['Yes', 'Yes', 'Yes', 'No']),
            'attendance_duration_min': random.randint(10, 60),
            'poll_responses': random.choice(['Yes', 'No']),
            'questions_asked': random.randint(0, 3),
            'follow_up_requested': random.choice(['Yes', 'No'])
        })
        attendance_id += 1
else:
    for _, event in webinar_events.iterrows():
        # Random attendees from leads
        num_attendees = random.randint(30, 200)
        attendee_leads = leads_df.sample(n=min(num_attendees, len(leads_df)))
        
        for _, lead in attendee_leads.iterrows():
            attended = random.choice([True, True, True, False])  # 75% attendance rate
            
            webinar_attendance_data.append({
                'attendance_id': generate_id('ATTEND', attendance_id),
                'event_id': event['event_id'],
                'lead_id': lead['lead_id'],
                'registration_date': random_date(event['event_date'] - timedelta(days=30), event['event_date']),
                'attended': 'Yes' if attended else 'No',
                'attendance_duration_min': random.randint(10, 60) if attended else 0,
                'poll_responses': 'Yes' if attended and random.random() > 0.5 else 'No',
                'questions_asked': random.randint(0, 3) if attended else 0,
                'follow_up_requested': 'Yes' if attended and random.random() > 0.7 else 'No'
            })
            attendance_id += 1

webinar_attendance_df = pd.DataFrame(webinar_attendance_data)
webinar_attendance_df.to_csv(f"{OUTPUT_DIR}/marketing_data/webinar_attendance.csv", index=False)
print(f"✓ Generated {len(webinar_attendance_df)} webinar attendance records")

# 44. CONTENT_ASSETS
print("Generating CONTENT_ASSETS table...")
content_assets_data = []
asset_types = ['Whitepaper', 'eBook', 'Video', 'Webinar', 'Case Study', 'Blog']
topics = ['AI/ML', 'Cloud Migration', 'Security', 'Data Analytics', 'Digital Transformation']

for i in range(150):
    publish_date = random_date(START_DATE, END_DATE)
    
    content_assets_data.append({
        'asset_id': generate_id('ASSET', i+1),
        'asset_name': f"{random.choice(topics)} - {random.choice(asset_types)} {i+1}",
        'asset_type': random.choice(asset_types),
        'product_focus': random.choice(list(products_df['product_id'])),
        'topic': random.choice(topics),
        'publish_date': publish_date,
        'author': f"Author_{random.randint(1, 20)}",
        'downloads_count': random.randint(10, 5000),
        'views_count': random.randint(50, 20000),
        'avg_time_spent_min': round(random.uniform(2, 30), 2),
        'conversion_rate': round(random.uniform(1, 25), 2),
        'gated': random.choice(['Yes', 'Yes', 'No']),
        'file_url': f"https://content.example.com/asset-{i+1}",
        'status': random.choice(['Published', 'Published', 'Published', 'Archived'])
    })

content_assets_df = pd.DataFrame(content_assets_data)
content_assets_df.to_csv(f"{OUTPUT_DIR}/marketing_data/content_assets.csv", index=False)
print(f"✓ Generated {len(content_assets_df)} content assets")

# 45. EMAIL_CAMPAIGNS
print("Generating EMAIL_CAMPAIGNS table...")
email_campaigns_data = []

for i in range(200):
    campaign = campaigns_df[campaigns_df['campaign_type'] == 'Email'].sample(1).iloc[0] if len(campaigns_df[campaigns_df['campaign_type'] == 'Email']) > 0 else campaigns_df.sample(1).iloc[0]
    send_date = random_date(campaign['start_date'], campaign['end_date'])
    recipient_count = random.randint(500, 50000)
    delivered = int(recipient_count * random.uniform(0.95, 0.99))
    opened = int(delivered * random.uniform(0.15, 0.35))
    clicked = int(opened * random.uniform(0.10, 0.30))
    
    email_campaigns_data.append({
        'email_campaign_id': generate_id('EMAIL', i+1),
        'campaign_id': campaign['campaign_id'],
        'email_name': f"Email: {campaign['campaign_name']} - Batch {i+1}",
        'send_date': send_date,
        'recipient_count': recipient_count,
        'delivered_count': delivered,
        'open_count': opened,
        'open_rate_percent': round((opened / delivered) * 100, 2),
        'click_count': clicked,
        'click_rate_percent': round((clicked / delivered) * 100, 2),
        'bounce_count': recipient_count - delivered,
        'unsubscribe_count': random.randint(0, int(recipient_count * 0.01)),
        'spam_complaint_count': random.randint(0, int(recipient_count * 0.001)),
        'conversion_count': random.randint(0, int(clicked * 0.15))
    })

email_campaigns_df = pd.DataFrame(email_campaigns_data)
email_campaigns_df.to_csv(f"{OUTPUT_DIR}/marketing_data/email_campaigns.csv", index=False)
print(f"✓ Generated {len(email_campaigns_df)} email campaigns")

# 46. SOCIAL_MEDIA_ENGAGEMENT
print("Generating SOCIAL_MEDIA_ENGAGEMENT table...")
social_data = []
platforms = ['LinkedIn', 'Twitter', 'Facebook', 'Instagram', 'YouTube']
post_types = ['Organic', 'Paid', 'Sponsored']
content_types = ['Post', 'Video', 'Article', 'Event', 'Poll']

for i in range(500):
    post_date = random_date(START_DATE, END_DATE)
    impressions = random.randint(1000, 100000)
    engagement = int(impressions * random.uniform(0.02, 0.10))
    
    social_data.append({
        'social_engagement_id': generate_id('SOCIAL', i+1),
        'campaign_id': random.choice(list(campaigns_df['campaign_id'])) if random.random() > 0.5 else None,
        'platform': random.choice(platforms),
        'post_date': post_date,
        'post_type': random.choice(post_types),
        'content_type': random.choice(content_types),
        'impressions': impressions,
        'reach': int(impressions * random.uniform(0.7, 0.95)),
        'engagement_count': engagement,
        'click_count': int(engagement * random.uniform(0.1, 0.3)),
        'video_views': random.randint(500, 50000) if random.random() > 0.7 else 0,
        'lead_count': random.randint(0, 50),
        'spend': round(random.uniform(100, 5000), 2) if post_types != 'Organic' else 0,
        'post_url': f"https://social.example.com/post-{i+1}"
    })

social_df = pd.DataFrame(social_data)
social_df.to_csv(f"{OUTPUT_DIR}/marketing_data/social_media_engagement.csv", index=False)
print(f"✓ Generated {len(social_df)} social media posts")

# 47. WEBSITE_TRAFFIC
print("Generating WEBSITE_TRAFFIC table...")
traffic_data = []
pages = ['/home', '/products', '/pricing', '/demo', '/blog', '/resources', '/contact', '/about']

traffic_id = 1
current_date = START_DATE
while current_date <= END_DATE:
    for page in pages:
        traffic_data.append({
            'traffic_id': generate_id('TRAFFIC', traffic_id),
            'traffic_date': current_date,
            'page_url': page,
            'page_views': random.randint(100, 10000),
            'unique_visitors': random.randint(50, 5000),
            'avg_time_on_page_sec': random.randint(30, 300),
            'bounce_rate_percent': round(random.uniform(20, 80), 2),
            'entry_page_count': random.randint(10, 1000),
            'exit_page_count': random.randint(10, 1000),
            'conversion_count': random.randint(0, 100),
            'source': random.choice(['Organic', 'Paid', 'Direct', 'Referral', 'Social']),
            'campaign_id': random.choice(list(campaigns_df['campaign_id'])) if random.random() > 0.7 else None
        })
        traffic_id += 1
    current_date += timedelta(days=7)  # Weekly

traffic_df = pd.DataFrame(traffic_data)
traffic_df.to_csv(f"{OUTPUT_DIR}/marketing_data/website_traffic.csv", index=False)
print(f"✓ Generated {len(traffic_df)} website traffic records")

# 48. PARTNER_REFERRALS
print("Generating PARTNER_REFERRALS table...")
partner_referrals_data = []
partner_types = ['Reseller', 'Technology', 'Consulting', 'Agency']
referral_statuses = ['New', 'Qualified', 'Converted', 'Lost']

for i in range(300):
    referral_date = random_date(START_DATE, END_DATE)
    status = random.choice(referral_statuses)
    
    partner_referrals_data.append({
        'referral_id': generate_id('REF', i+1),
        'partner_name': f"Partner Company {i+1}",
        'partner_type': random.choice(partner_types),
        'referral_date': referral_date,
        'contact_name': f"Referral Contact {i+1}",
        'contact_email': f"referral{i+1}@company{i+1}.com",
        'company_name': f"Referred Company {i+1}",
        'industry': random.choice(['Technology', 'Healthcare', 'Finance', 'Retail']),
        'region': random.choice(['North America', 'EMEA', 'APAC', 'Latin America']),
        'estimated_value': round(random.uniform(10000, 500000), 2),
        'status': status,
        'lead_id': random.choice(list(leads_df['lead_id'])) if status in ['Qualified', 'Converted'] else None,
        'opportunity_id': random.choice(list(opportunities_df['opportunity_id'])) if status == 'Converted' else None,
        'commission_percent': round(random.uniform(5, 20), 2),
        'notes': f"Partner referral notes #{i+1}"
    })

partner_referrals_df = pd.DataFrame(partner_referrals_data)
partner_referrals_df.to_csv(f"{OUTPUT_DIR}/marketing_data/partner_referrals.csv", index=False)
print(f"✓ Generated {len(partner_referrals_df)} partner referrals")

# 49. MARKETING_BUDGET
print("Generating MARKETING_BUDGET table...")
budget_data = []
budget_id = 1
channels = ['Digital', 'Events', 'Content', 'PR', 'Partner']
campaign_types = ['Email', 'Social', 'Webinar', 'Conference', 'Content Syndication']

for year in [2023, 2024, 2025]:
    for q in range(1, 5):
        quarter = f"Q{q} {year}"
        for channel in channels:
            for camp_type in random.sample(campaign_types, 3):
                budgeted = round(random.uniform(10000, 200000), 2)
                actual = round(budgeted * random.uniform(0.8, 1.1), 2)
                leads = random.randint(50, 2000)
                opps = int(leads * random.uniform(0.1, 0.3))
                pipeline = round(opps * random.uniform(50000, 500000), 2)
                closed = round(pipeline * random.uniform(0.15, 0.35), 2)
                
                budget_data.append({
                    'budget_id': generate_id('BUDG', budget_id),
                    'fiscal_quarter': quarter,
                    'fiscal_year': year,
                    'channel': channel,
                    'campaign_type': camp_type,
                    'budgeted_amount': budgeted,
                    'actual_spend': actual,
                    'variance_amount': actual - budgeted,
                    'leads_generated': leads,
                    'opportunities_generated': opps,
                    'pipeline_value': pipeline,
                    'closed_won_value': closed,
                    'roi_percent': min(round((closed / actual) * 100, 2) if actual > 0 else 0, 999.99)  # Cap at 999.99 for NUMBER(5,2)
                })
                budget_id += 1

budget_df = pd.DataFrame(budget_data)
budget_df.to_csv(f"{OUTPUT_DIR}/marketing_data/marketing_budget.csv", index=False)
print(f"✓ Generated {len(budget_df)} budget records")

# 50. LEAD_SCORING_RULES
print("Generating LEAD_SCORING_RULES table...")
scoring_rules_data = []
categories = ['Demographic', 'Behavioral', 'Firmographic']
criteria_examples = {
    'Demographic': ['Job Title = VP', 'Job Title = Director', 'Job Title = Manager', 'Job Level = C-Level'],
    'Behavioral': ['Downloaded Whitepaper', 'Attended Webinar', 'Visited Pricing Page', 'Requested Demo', 'Opened Email'],
    'Firmographic': ['Company Size > 1000', 'Industry = Technology', 'Annual Revenue > $100M', 'Region = North America']
}

rule_id = 1
for category in categories:
    for criteria in criteria_examples[category]:
        scoring_rules_data.append({
            'rule_id': generate_id('RULE', rule_id),
            'rule_name': f"{category} - {criteria}",
            'rule_category': category,
            'criteria': criteria,
            'points': random.choice([5, 10, 15, 20, 25]),
            'active': random.choice(['Yes', 'Yes', 'Yes', 'No']),
            'effective_date': random_date(START_DATE, END_DATE - timedelta(days=180)),
            'created_by': f"Marketing_Ops_{random.randint(1, 5)}",
            'last_modified': random_date(START_DATE, END_DATE)
        })
        rule_id += 1

scoring_rules_df = pd.DataFrame(scoring_rules_data)
scoring_rules_df.to_csv(f"{OUTPUT_DIR}/marketing_data/lead_scoring_rules.csv", index=False)
print(f"✓ Generated {len(scoring_rules_df)} scoring rules")

print("\n=== Generating Additional Support Tables ===")

# 10. TELEMETRY_EVENTS
print("Generating TELEMETRY_EVENTS table...")
telemetry_data = []
event_types = ['Login', 'Logout', 'Error', 'Feature_Used', 'Export', 'Integration', 'Config_Change']
severities = ['Info', 'Warning', 'Error', 'Critical']

for i in range(100000):
    install = customer_products_df.sample(1).iloc[0]
    event_time = random_date(install['install_date'], END_DATE)
    
    telemetry_data.append({
        'event_id': generate_id('TELEM', i+1),
        'customer_id': install['customer_id'],
        'product_id': install['product_id'],
        'event_timestamp': event_time,
        'event_type': random.choice(event_types),
        'event_category': random.choice(['Usage', 'Performance', 'Security', 'Integration']),
        'severity': random.choice(severities),
        'user_id': f"user_{random.randint(1, 10000)}",
        'session_id': f"session_{random.randint(1, 100000)}",
        'metadata_json': json.dumps({'key': 'value', 'detail': f'event_{i}'})
    })

telemetry_df = pd.DataFrame(telemetry_data)
telemetry_df.to_csv(f"{OUTPUT_DIR}/usage_data/telemetry_events.csv", index=False)
print(f"✓ Generated {len(telemetry_df)} telemetry events")

# 11. PRODUCT_HEALTH_ALERTS
print("Generating PRODUCT_HEALTH_ALERTS table...")
alert_data = []
alert_types = ['Low Usage', 'High Error Rate', 'License Expiring', 'Version Outdated', 'Integration Failed']

for i in range(1000):
    install = customer_products_df.sample(1).iloc[0]
    alert_date = random_date(install['install_date'], END_DATE)
    status = random.choice(['Open', 'Acknowledged', 'Resolved', 'Resolved', 'Resolved'])
    
    alert_data.append({
        'alert_id': generate_id('ALERT', i+1),
        'customer_id': install['customer_id'],
        'product_id': install['product_id'],
        'alert_date': alert_date,
        'alert_type': random.choice(alert_types),
        'severity': random.choice(['Low', 'Medium', 'High', 'Critical']),
        'status': status,
        'assigned_to': f"CSM_{random.randint(1, 15)}",
        'resolution_date': alert_date + timedelta(days=random.randint(1, 30)) if status == 'Resolved' else None
    })

alert_df = pd.DataFrame(alert_data)
alert_df.to_csv(f"{OUTPUT_DIR}/usage_data/product_health_alerts.csv", index=False)
print(f"✓ Generated {len(alert_df)} product health alerts")

# 19. TICKET_COMMENTS
print("Generating TICKET_COMMENTS table...")
comments_data = []
comment_id = 1

for _, ticket in support_tickets_df.head(5000).iterrows():
    # Each ticket gets 2-8 comments
    num_comments = random.randint(2, 8)
    for i in range(num_comments):
        comment_date = random_date(ticket['created_date'], 
                                   ticket['closed_date'] if pd.notna(ticket['closed_date']) else END_DATE)
        
        comments_data.append({
            'comment_id': generate_id('COMM', comment_id),
            'ticket_id': ticket['ticket_id'],
            'comment_date': comment_date,
            'commenter_type': random.choice(['Customer', 'Support Agent', 'Engineer', 'Manager']),
            'commenter_name': f"Person_{random.randint(1, 100)}",
            'comment_text': f"Comment text for ticket {ticket['ticket_number']}",
            'internal_note': random.choice(['Yes', 'No']),
            'time_spent_min': random.randint(5, 120)
        })
        comment_id += 1

comments_df = pd.DataFrame(comments_data)
comments_df.to_csv(f"{OUTPUT_DIR}/support_data/ticket_comments.csv", index=False)
print(f"✓ Generated {len(comments_df)} ticket comments")

# 20. TICKET_RESOLUTION
print("Generating TICKET_RESOLUTION table...")
resolution_data = []

resolved_tickets = support_tickets_df[support_tickets_df['status'].isin(['Resolved', 'Closed'])]
for i, (_, ticket) in enumerate(resolved_tickets.iterrows()):
    resolution_data.append({
        'resolution_id': generate_id('RES', i+1),
        'ticket_id': ticket['ticket_id'],
        'resolution_date': ticket['resolved_date'],
        'resolution_type': random.choice(['Fixed', 'Workaround', 'By Design', 'Duplicate', 'Cannot Reproduce', 'Configuration']),
        'resolution_notes': f"Resolution for {ticket['ticket_number']}",
        'root_cause': random.choice(['Product Bug', 'User Error', 'Configuration', 'Infrastructure', 'Documentation']),
        'kb_article_id': f"KB-{random.randint(1, 300)}" if random.random() > 0.6 else None,
        'workaround_provided': random.choice(['Yes', 'No']),
        'permanent_fix': random.choice(['Yes', 'No']),
        'engineer_assigned': ticket['assigned_to'],
        'effort_hours': round(random.uniform(0.5, 40), 1)
    })

resolution_df = pd.DataFrame(resolution_data)
resolution_df.to_csv(f"{OUTPUT_DIR}/support_data/ticket_resolution.csv", index=False)
print(f"✓ Generated {len(resolution_df)} ticket resolutions")

# 22. SLA_POLICIES
print("Generating SLA_POLICIES table...")
sla_policies_data = []
policy_id = 1

tiers = ['Bronze', 'Silver', 'Gold', 'Platinum']
priorities = ['Low', 'Medium', 'High', 'Critical']

for tier in tiers:
    for priority in priorities:
        if tier == 'Platinum' and priority == 'Critical':
            first_response = 15
            resolution = 4
        elif tier == 'Gold' and priority == 'Critical':
            first_response = 30
            resolution = 8
        elif priority == 'Critical':
            first_response = 60
            resolution = 24
        elif priority == 'High':
            first_response = 120
            resolution = 48
        elif priority == 'Medium':
            first_response = 240
            resolution = 72
        else:
            first_response = 480
            resolution = 120
        
        sla_policies_data.append({
            'sla_policy_id': generate_id('SLA', policy_id),
            'customer_tier': tier,
            'priority': priority,
            'first_response_target_min': first_response,
            'resolution_target_hours': resolution,
            'business_hours_only': 'Yes' if tier in ['Bronze', 'Silver'] else 'No',
            'active': 'Yes',
            'effective_date': START_DATE
        })
        policy_id += 1

sla_policies_df = pd.DataFrame(sla_policies_data)
sla_policies_df.to_csv(f"{OUTPUT_DIR}/support_data/sla_policies.csv", index=False)
print(f"✓ Generated {len(sla_policies_df)} SLA policies")

# 23. SLA_BREACHES
print("Generating SLA_BREACHES table...")
breach_data = []

breached_tickets = support_tickets_df[support_tickets_df['sla_status'] == 'Breached']
for i, (_, ticket) in enumerate(breached_tickets.iterrows()):
    breach_data.append({
        'breach_id': generate_id('BREACH', i+1),
        'ticket_id': ticket['ticket_id'],
        'customer_id': ticket['customer_id'],
        'breach_type': random.choice(['First Response', 'Resolution Time']),
        'target_time': random.randint(30, 240),
        'actual_time': random.randint(240, 600),
        'breach_duration_min': random.randint(10, 360),
        'breach_date': random_date(ticket['created_date'], ticket['closed_date'] if pd.notna(ticket['closed_date']) else END_DATE),
        'reason': random.choice(['High Volume', 'Complexity', 'Resource Unavailable', 'Escalation Delay']),
        'credited': random.choice(['Yes', 'No'])
    })

breach_df = pd.DataFrame(breach_data)
breach_df.to_csv(f"{OUTPUT_DIR}/support_data/sla_breaches.csv", index=False)
print(f"✓ Generated {len(breach_df)} SLA breaches")

# Continue with remaining tables...
print("\n✓ Part 3 Generation Complete!")
print("All 62 tables generated successfully!")
print(f"\nNext steps:")
print("1. Review generated CSV files in {OUTPUT_DIR}/")
print("2. Upload to S3 or cloud storage")
print("3. Use Snowflake/Databricks loading scripts")