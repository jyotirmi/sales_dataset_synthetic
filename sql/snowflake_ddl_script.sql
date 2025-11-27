-- ============================================================================
-- Snowflake DDL Script - Complete 62 Tables
-- Sales Analytics Demo Dataset
-- ============================================================================

-- STEP 1: Create Database and Schemas
-- ============================================================================

CREATE DATABASE IF NOT EXISTS sales_analytics_demo;
USE DATABASE sales_analytics_demo;

CREATE SCHEMA IF NOT EXISTS sales_data;
CREATE SCHEMA IF NOT EXISTS usage_data;
CREATE SCHEMA IF NOT EXISTS marketing_data;
CREATE SCHEMA IF NOT EXISTS support_data;
CREATE SCHEMA IF NOT EXISTS operational_data;

-- STEP 2: Create File Format
-- ============================================================================

CREATE OR REPLACE FILE FORMAT sales_analytics_demo.public.csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE;

-- STEP 3: Create S3 External Stage
-- ============================================================================

-- Option A: External S3 Stage (recommended)
-- CREATE OR REPLACE STAGE sales_analytics_demo.public.s3_stage
--   URL = 's3://your-bucket-name/sales-demo-data/'
--   CREDENTIALS = (AWS_KEY_ID = 'your_access_key' AWS_SECRET_KEY = 'your_secret_key')
--   FILE_FORMAT = sales_analytics_demo.public.csv_format;

-- Option B: Internal Stage (for local file upload)
CREATE OR REPLACE STAGE sales_analytics_demo.public.csv_stage
  FILE_FORMAT = sales_analytics_demo.public.csv_format;

-- ============================================================================
-- SALES_DATA SCHEMA - 17 Tables
-- ============================================================================

USE SCHEMA sales_analytics_demo.sales_data;

-- 1. CUSTOMERS
CREATE OR REPLACE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(200),
    industry VARCHAR(100),
    company_size VARCHAR(50),
    region VARCHAR(100),
    country VARCHAR(100),
    annual_revenue NUMBER(15,2),
    account_owner VARCHAR(100),
    created_date DATE,
    customer_tier VARCHAR(50)
);

-- 2. PRODUCTS
CREATE OR REPLACE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(200),
    product_category VARCHAR(100),
    current_version VARCHAR(50),
    unit_price NUMBER(15,2),
    product_line VARCHAR(50),
    release_date DATE
);

-- 3. CUSTOMER_PRODUCTS
CREATE OR REPLACE TABLE customer_products (
    install_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    version_number VARCHAR(50),
    license_count INTEGER,
    install_date DATE,
    contract_value NUMBER(15,2),
    renewal_date DATE,
    usage_tier VARCHAR(50),
    support_level VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 4. OPPORTUNITIES
CREATE OR REPLACE TABLE opportunities (
    opportunity_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    opportunity_name VARCHAR(500),
    stage VARCHAR(100),
    amount NUMBER(15,2),
    probability INTEGER,
    close_date DATE,
    created_date DATE,
    opportunity_type VARCHAR(100),
    region VARCHAR(100),
    sales_rep VARCHAR(100),
    lead_source VARCHAR(100),
    competitor VARCHAR(200),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 5. CLOSED_DEALS
CREATE OR REPLACE TABLE closed_deals (
    deal_id VARCHAR(50) PRIMARY KEY,
    opportunity_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    deal_value NUMBER(15,2),
    close_date DATE,
    quarter VARCHAR(20),
    fiscal_year INTEGER,
    status VARCHAR(20),
    win_loss_reason VARCHAR(200),
    sales_cycle_days INTEGER,
    discount_percent NUMBER(5,2),
    region VARCHAR(100),
    sales_rep VARCHAR(100),
    FOREIGN KEY (opportunity_id) REFERENCES opportunities(opportunity_id)
);

-- 6. SALES_TARGETS
CREATE OR REPLACE TABLE sales_targets (
    target_id VARCHAR(50) PRIMARY KEY,
    quarter VARCHAR(20),
    region VARCHAR(100),
    product_id VARCHAR(50),
    target_amount NUMBER(15,2),
    sales_rep VARCHAR(100)
);

-- 7. CUSTOMER_ATTRIBUTES
CREATE OR REPLACE TABLE customer_attributes (
    attribute_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    attribute_type VARCHAR(100),
    attribute_value VARCHAR(200),
    created_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- 29. CUSTOMER_CONTACTS
CREATE OR REPLACE TABLE customer_contacts (
    contact_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    phone VARCHAR(50),
    title VARCHAR(100),
    department VARCHAR(100),
    role_type VARCHAR(50),
    is_primary VARCHAR(10),
    linkedin_url VARCHAR(500),
    last_contact_date DATE,
    created_date DATE,
    status VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- 30. ACCOUNT_HIERARCHY
CREATE OR REPLACE TABLE account_hierarchy (
    hierarchy_id VARCHAR(50) PRIMARY KEY,
    parent_customer_id VARCHAR(50),
    child_customer_id VARCHAR(50),
    relationship_type VARCHAR(100),
    ownership_percent NUMBER(5,2),
    created_date DATE
);

-- 31. CUSTOMER_NOTES
CREATE OR REPLACE TABLE customer_notes (
    note_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    contact_id VARCHAR(50),
    note_date DATE,
    note_type VARCHAR(50),
    created_by VARCHAR(100),
    subject VARCHAR(500),
    note_text TEXT,
    opportunity_id VARCHAR(50),
    next_action VARCHAR(200),
    next_action_date DATE
);

-- 32. COMPETITIVE_INTELLIGENCE
CREATE OR REPLACE TABLE competitive_intelligence (
    intel_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    opportunity_id VARCHAR(50),
    competitor_name VARCHAR(200),
    competitor_product VARCHAR(200),
    intel_date DATE,
    intel_source VARCHAR(100),
    competitor_strength VARCHAR(200),
    competitor_weakness VARCHAR(200),
    our_position VARCHAR(50),
    intel_type VARCHAR(100),
    notes TEXT
);

-- 33. QUOTES
CREATE OR REPLACE TABLE quotes (
    quote_id VARCHAR(50) PRIMARY KEY,
    quote_number VARCHAR(50),
    opportunity_id VARCHAR(50),
    customer_id VARCHAR(50),
    quote_date DATE,
    expiration_date DATE,
    total_amount NUMBER(15,2),
    discount_percent NUMBER(5,2),
    payment_terms VARCHAR(50),
    delivery_terms VARCHAR(100),
    status VARCHAR(50),
    created_by VARCHAR(100),
    approved_by VARCHAR(100),
    approval_date DATE,
    notes TEXT
);

-- 34. QUOTE_LINE_ITEMS
CREATE OR REPLACE TABLE quote_line_items (
    line_item_id VARCHAR(50) PRIMARY KEY,
    quote_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INTEGER,
    unit_price NUMBER(15,2),
    discount_percent NUMBER(5,2),
    line_total NUMBER(15,2),
    description VARCHAR(500),
    product_version VARCHAR(50),
    license_type VARCHAR(50),
    term_months INTEGER
);

-- 35. CONTRACTS
CREATE OR REPLACE TABLE contracts (
    contract_id VARCHAR(50) PRIMARY KEY,
    contract_number VARCHAR(50),
    customer_id VARCHAR(50),
    opportunity_id VARCHAR(50),
    quote_id VARCHAR(50),
    contract_type VARCHAR(100),
    start_date DATE,
    end_date DATE,
    contract_value NUMBER(15,2),
    annual_value NUMBER(15,2),
    payment_schedule VARCHAR(50),
    auto_renew VARCHAR(10),
    renewal_notice_days INTEGER,
    status VARCHAR(50),
    signed_date DATE,
    signed_by_customer VARCHAR(200),
    signed_by_vendor VARCHAR(200)
);

-- 36. REVENUE_RECOGNITION
CREATE OR REPLACE TABLE revenue_recognition (
    recognition_id VARCHAR(50) PRIMARY KEY,
    contract_id VARCHAR(50),
    customer_id VARCHAR(50),
    recognition_date DATE,
    recognition_amount NUMBER(15,2),
    recognition_type VARCHAR(50),
    fiscal_quarter VARCHAR(20),
    fiscal_year INTEGER,
    product_id VARCHAR(50),
    region VARCHAR(100),
    deferred_revenue NUMBER(15,2),
    recognized_to_date NUMBER(15,2)
);

-- 37. TERRITORIES
CREATE OR REPLACE TABLE territories (
    territory_id VARCHAR(50) PRIMARY KEY,
    territory_name VARCHAR(200),
    region VARCHAR(100),
    country_list VARCHAR(1000),
    territory_type VARCHAR(100),
    sales_rep VARCHAR(100),
    manager VARCHAR(100),
    quota NUMBER(15,2),
    active VARCHAR(10),
    effective_date DATE
);

-- ============================================================================
-- USAGE_DATA SCHEMA - 11 Tables
-- ============================================================================

USE SCHEMA sales_analytics_demo.usage_data;

-- 8. PRODUCT_USAGE_SUMMARY
CREATE OR REPLACE TABLE product_usage_summary (
    usage_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    usage_date DATE,
    active_users INTEGER,
    total_sessions INTEGER,
    avg_session_duration_min NUMBER(10,2),
    feature_adoption_score NUMBER(5,2),
    data_volume_gb NUMBER(15,2),
    api_calls INTEGER,
    error_rate_percent NUMBER(5,2),
    login_count INTEGER,
    license_utilization NUMBER(5,2),
    health_score VARCHAR(50)
);

-- 9. FEATURE_USAGE
CREATE OR REPLACE TABLE feature_usage (
    feature_usage_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    feature_name VARCHAR(200),
    usage_date DATE,
    usage_count INTEGER,
    unique_users INTEGER,
    adoption_status VARCHAR(50)
);

-- 10. TELEMETRY_EVENTS
CREATE OR REPLACE TABLE telemetry_events (
    event_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    event_timestamp TIMESTAMP,
    event_type VARCHAR(100),
    event_category VARCHAR(100),
    severity VARCHAR(50),
    user_id VARCHAR(100),
    session_id VARCHAR(100),
    metadata_json VARIANT
);

-- 11. PRODUCT_HEALTH_ALERTS
CREATE OR REPLACE TABLE product_health_alerts (
    alert_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    alert_date DATE,
    alert_type VARCHAR(200),
    severity VARCHAR(50),
    status VARCHAR(50),
    assigned_to VARCHAR(100),
    resolution_date DATE
);

-- 38. USER_SESSIONS
CREATE OR REPLACE TABLE user_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    user_id VARCHAR(100),
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    duration_minutes INTEGER,
    pages_viewed INTEGER,
    actions_taken INTEGER,
    device_type VARCHAR(50),
    browser VARCHAR(50),
    os VARCHAR(50),
    ip_address VARCHAR(50),
    location_city VARCHAR(100),
    location_country VARCHAR(100)
);

-- 39. API_USAGE
CREATE OR REPLACE TABLE api_usage (
    api_usage_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    usage_date DATE,
    endpoint VARCHAR(200),
    method VARCHAR(20),
    call_count INTEGER,
    success_count INTEGER,
    error_count INTEGER,
    avg_response_time_ms NUMBER(10,2),
    data_transferred_gb NUMBER(15,2),
    authentication_type VARCHAR(50)
);

-- 40. INTEGRATION_USAGE
CREATE OR REPLACE TABLE integration_usage (
    integration_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    integration_name VARCHAR(200),
    integration_type VARCHAR(100),
    status VARCHAR(50),
    install_date DATE,
    last_sync_date DATE,
    sync_frequency VARCHAR(50),
    records_synced INTEGER,
    error_count INTEGER,
    config_json VARIANT
);

-- 41. LICENSE_USAGE
CREATE OR REPLACE TABLE license_usage (
    license_usage_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    usage_date DATE,
    licenses_purchased INTEGER,
    licenses_active INTEGER,
    licenses_available INTEGER,
    utilization_percent NUMBER(5,2),
    peak_usage_date DATE,
    peak_usage_count INTEGER,
    overage_count INTEGER,
    overage_charges NUMBER(15,2)
);

-- 42. DATA_QUALITY_METRICS
CREATE OR REPLACE TABLE data_quality_metrics (
    quality_metric_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    metric_date DATE,
    completeness_score NUMBER(5,2),
    accuracy_score NUMBER(5,2),
    consistency_score NUMBER(5,2),
    timeliness_score NUMBER(5,2),
    duplicate_records INTEGER,
    null_values_percent NUMBER(5,2),
    validation_errors INTEGER,
    overall_quality_score NUMBER(5,2)
);

-- 43. PERFORMANCE_METRICS
CREATE OR REPLACE TABLE performance_metrics (
    performance_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    metric_date DATE,
    avg_page_load_time_ms NUMBER(10,2),
    avg_query_time_ms NUMBER(10,2),
    uptime_percent NUMBER(5,2),
    downtime_minutes INTEGER,
    peak_concurrent_users INTEGER,
    avg_concurrent_users INTEGER,
    cpu_utilization_percent NUMBER(5,2),
    memory_utilization_percent NUMBER(5,2),
    storage_used_gb NUMBER(15,2)
);

-- ============================================================================
-- MARKETING_DATA SCHEMA - 13 Tables
-- ============================================================================

USE SCHEMA sales_analytics_demo.marketing_data;

-- 12. CAMPAIGNS
CREATE OR REPLACE TABLE campaigns (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(500),
    campaign_type VARCHAR(100),
    channel VARCHAR(100),
    start_date DATE,
    end_date DATE,
    budget NUMBER(15,2),
    target_audience VARCHAR(100),
    region VARCHAR(100),
    product_focus VARCHAR(50),
    status VARCHAR(50),
    owner VARCHAR(100)
);

-- 13. EVENTS
CREATE OR REPLACE TABLE events (
    event_id VARCHAR(50) PRIMARY KEY,
    event_name VARCHAR(500),
    event_type VARCHAR(100),
    event_date DATE,
    location VARCHAR(200),
    region VARCHAR(100),
    expected_attendees INTEGER,
    actual_attendees INTEGER,
    budget NUMBER(15,2),
    products_showcased VARCHAR(1000),
    lead_goal INTEGER,
    campaign_id VARCHAR(50)
);

-- 14. LEADS
CREATE OR REPLACE TABLE leads (
    lead_id VARCHAR(50) PRIMARY KEY,
    company_name VARCHAR(200),
    contact_name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    title VARCHAR(100),
    industry VARCHAR(100),
    company_size VARCHAR(50),
    region VARCHAR(100),
    country VARCHAR(100),
    lead_source VARCHAR(100),
    campaign_id VARCHAR(50),
    event_id VARCHAR(50),
    lead_score INTEGER,
    lead_status VARCHAR(50),
    created_date DATE,
    last_activity_date DATE,
    assigned_to VARCHAR(100),
    products_interested VARCHAR(500),
    converted_to_opp_id VARCHAR(50),
    conversion_date DATE
);

-- 15. CAMPAIGN_ENGAGEMENT
CREATE OR REPLACE TABLE campaign_engagement (
    engagement_id VARCHAR(50) PRIMARY KEY,
    campaign_id VARCHAR(50),
    lead_id VARCHAR(50),
    contact_id VARCHAR(200),
    engagement_type VARCHAR(100),
    engagement_date DATE,
    asset_name VARCHAR(500),
    engagement_score INTEGER
);

-- 16. MARKETING_ATTRIBUTION
CREATE OR REPLACE TABLE marketing_attribution (
    attribution_id VARCHAR(50) PRIMARY KEY,
    opportunity_id VARCHAR(50),
    deal_id VARCHAR(50),
    campaign_id VARCHAR(50),
    event_id VARCHAR(50),
    touchpoint_order INTEGER,
    touchpoint_date DATE,
    attribution_weight NUMBER(5,2),
    attribution_model VARCHAR(100)
);

-- 17. WEBINAR_ATTENDANCE
CREATE OR REPLACE TABLE webinar_attendance (
    attendance_id VARCHAR(50) PRIMARY KEY,
    event_id VARCHAR(50),
    lead_id VARCHAR(50),
    registration_date DATE,
    attended VARCHAR(10),
    attendance_duration_min INTEGER,
    poll_responses VARCHAR(10),
    questions_asked INTEGER,
    follow_up_requested VARCHAR(10)
);

-- 44. CONTENT_ASSETS
CREATE OR REPLACE TABLE content_assets (
    asset_id VARCHAR(50) PRIMARY KEY,
    asset_name VARCHAR(500),
    asset_type VARCHAR(100),
    product_focus VARCHAR(50),
    topic VARCHAR(200),
    publish_date DATE,
    author VARCHAR(100),
    downloads_count INTEGER,
    views_count INTEGER,
    avg_time_spent_min NUMBER(10,2),
    conversion_rate NUMBER(5,2),
    gated VARCHAR(10),
    file_url VARCHAR(500),
    status VARCHAR(50)
);

-- 45. EMAIL_CAMPAIGNS
CREATE OR REPLACE TABLE email_campaigns (
    email_campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_id VARCHAR(50),
    email_name VARCHAR(500),
    send_date DATE,
    recipient_count INTEGER,
    delivered_count INTEGER,
    open_count INTEGER,
    open_rate_percent NUMBER(5,2),
    click_count INTEGER,
    click_rate_percent NUMBER(5,2),
    bounce_count INTEGER,
    unsubscribe_count INTEGER,
    spam_complaint_count INTEGER,
    conversion_count INTEGER
);

-- 46. SOCIAL_MEDIA_ENGAGEMENT
CREATE OR REPLACE TABLE social_media_engagement (
    social_engagement_id VARCHAR(50) PRIMARY KEY,
    campaign_id VARCHAR(50),
    platform VARCHAR(100),
    post_date DATE,
    post_type VARCHAR(50),
    content_type VARCHAR(50),
    impressions INTEGER,
    reach INTEGER,
    engagement_count INTEGER,
    click_count INTEGER,
    video_views INTEGER,
    lead_count INTEGER,
    spend NUMBER(15,2),
    post_url VARCHAR(500)
);

-- 47. WEBSITE_TRAFFIC
CREATE OR REPLACE TABLE website_traffic (
    traffic_id VARCHAR(50) PRIMARY KEY,
    traffic_date DATE,
    page_url VARCHAR(500),
    page_views INTEGER,
    unique_visitors INTEGER,
    avg_time_on_page_sec INTEGER,
    bounce_rate_percent NUMBER(5,2),
    entry_page_count INTEGER,
    exit_page_count INTEGER,
    conversion_count INTEGER,
    source VARCHAR(100),
    campaign_id VARCHAR(50)
);

-- 48. PARTNER_REFERRALS
CREATE OR REPLACE TABLE partner_referrals (
    referral_id VARCHAR(50) PRIMARY KEY,
    partner_name VARCHAR(200),
    partner_type VARCHAR(100),
    referral_date DATE,
    contact_name VARCHAR(200),
    contact_email VARCHAR(200),
    company_name VARCHAR(200),
    industry VARCHAR(100),
    region VARCHAR(100),
    estimated_value NUMBER(15,2),
    status VARCHAR(50),
    lead_id VARCHAR(50),
    opportunity_id VARCHAR(50),
    commission_percent NUMBER(5,2),
    notes TEXT
);

-- 49. MARKETING_BUDGET
CREATE OR REPLACE TABLE marketing_budget (
    budget_id VARCHAR(50) PRIMARY KEY,
    fiscal_quarter VARCHAR(20),
    fiscal_year INTEGER,
    channel VARCHAR(100),
    campaign_type VARCHAR(100),
    budgeted_amount NUMBER(15,2),
    actual_spend NUMBER(15,2),
    variance_amount NUMBER(15,2),
    leads_generated INTEGER,
    opportunities_generated INTEGER,
    pipeline_value NUMBER(15,2),
    closed_won_value NUMBER(15,2),
    roi_percent NUMBER(5,2)
);

-- 50. LEAD_SCORING_RULES
CREATE OR REPLACE TABLE lead_scoring_rules (
    rule_id VARCHAR(50) PRIMARY KEY,
    rule_name VARCHAR(200),
    rule_category VARCHAR(100),
    criteria VARCHAR(500),
    points INTEGER,
    active VARCHAR(10),
    effective_date DATE,
    created_by VARCHAR(100),
    last_modified DATE
);

-- ============================================================================
-- SUPPORT_DATA SCHEMA - 13 Tables
-- ============================================================================

USE SCHEMA sales_analytics_demo.support_data;

-- 18. SUPPORT_TICKETS
CREATE OR REPLACE TABLE support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    ticket_number VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    contact_name VARCHAR(200),
    contact_email VARCHAR(200),
    subject VARCHAR(500),
    description TEXT,
    priority VARCHAR(50),
    severity VARCHAR(50),
    status VARCHAR(50),
    ticket_type VARCHAR(100),
    category VARCHAR(100),
    created_date DATE,
    first_response_date DATE,
    resolved_date DATE,
    closed_date DATE,
    assigned_to VARCHAR(100),
    escalated VARCHAR(10),
    escalation_date DATE,
    sla_status VARCHAR(50),
    channel VARCHAR(50),
    product_version VARCHAR(50),
    environment VARCHAR(50)
);

-- 19. TICKET_COMMENTS
CREATE OR REPLACE TABLE ticket_comments (
    comment_id VARCHAR(50) PRIMARY KEY,
    ticket_id VARCHAR(50),
    comment_date DATE,
    commenter_type VARCHAR(50),
    commenter_name VARCHAR(100),
    comment_text TEXT,
    internal_note VARCHAR(10),
    time_spent_min INTEGER
);

-- 20. TICKET_RESOLUTION
CREATE OR REPLACE TABLE ticket_resolution (
    resolution_id VARCHAR(50) PRIMARY KEY,
    ticket_id VARCHAR(50),
    resolution_date DATE,
    resolution_type VARCHAR(100),
    resolution_notes TEXT,
    root_cause VARCHAR(200),
    kb_article_id VARCHAR(50),
    workaround_provided VARCHAR(10),
    permanent_fix VARCHAR(10),
    engineer_assigned VARCHAR(100),
    effort_hours NUMBER(10,1)
);

-- 21. CUSTOMER_SATISFACTION
CREATE OR REPLACE TABLE customer_satisfaction (
    csat_id VARCHAR(50) PRIMARY KEY,
    ticket_id VARCHAR(50),
    customer_id VARCHAR(50),
    survey_date DATE,
    response_date DATE,
    satisfaction_score INTEGER,
    would_recommend VARCHAR(10),
    feedback_text TEXT,
    response_time_rating INTEGER,
    resolution_quality_rating INTEGER,
    agent_rating INTEGER,
    survey_type VARCHAR(50)
);

-- 22. SLA_POLICIES
CREATE OR REPLACE TABLE sla_policies (
    sla_policy_id VARCHAR(50) PRIMARY KEY,
    customer_tier VARCHAR(50),
    priority VARCHAR(50),
    first_response_target_min INTEGER,
    resolution_target_hours INTEGER,
    business_hours_only VARCHAR(10),
    active VARCHAR(10),
    effective_date DATE
);

-- 23. SLA_BREACHES
CREATE OR REPLACE TABLE sla_breaches (
    breach_id VARCHAR(50) PRIMARY KEY,
    ticket_id VARCHAR(50),
    customer_id VARCHAR(50),
    breach_type VARCHAR(50),
    target_time INTEGER,
    actual_time INTEGER,
    breach_duration_min INTEGER,
    breach_date DATE,
    reason VARCHAR(200),
    credited VARCHAR(10)
);

-- 24. KNOWLEDGE_BASE
CREATE OR REPLACE TABLE knowledge_base (
    kb_id VARCHAR(50) PRIMARY KEY,
    article_title VARCHAR(500),
    article_category VARCHAR(100),
    product_id VARCHAR(50),
    product_version VARCHAR(50),
    content TEXT,
    created_date DATE,
    last_updated DATE,
    author VARCHAR(100),
    views_count INTEGER,
    helpfulness_score NUMBER(5,2),
    related_tickets_count INTEGER,
    status VARCHAR(50),
    tags VARCHAR(500)
);

-- 25. TICKET_TAGS
CREATE OR REPLACE TABLE ticket_tags (
    tag_id VARCHAR(50) PRIMARY KEY,
    ticket_id VARCHAR(50),
    tag_name VARCHAR(100),
    tagged_by VARCHAR(100),
    tagged_date DATE
);

-- 26. ESCALATIONS
CREATE OR REPLACE TABLE escalations (
    escalation_id VARCHAR(50) PRIMARY KEY,
    ticket_id VARCHAR(50),
    customer_id VARCHAR(50),
    escalation_level VARCHAR(100),
    escalated_from VARCHAR(100),
    escalated_to VARCHAR(100),
    escalation_date DATE,
    escalation_reason VARCHAR(200),
    resolved_at_level VARCHAR(100),
    resolution_date DATE
);

-- 27. RECURRING_ISSUES
CREATE OR REPLACE TABLE recurring_issues (
    recurring_issue_id VARCHAR(50) PRIMARY KEY,
    issue_pattern VARCHAR(500),
    product_id VARCHAR(50),
    product_version VARCHAR(50),
    first_occurrence DATE,
    last_occurrence DATE,
    occurrence_count INTEGER,
    affected_customers_count INTEGER,
    priority VARCHAR(50),
    status VARCHAR(50),
    assigned_team VARCHAR(100),
    related_tickets VARCHAR(1000),
    root_cause TEXT,
    fix_version VARCHAR(50)
);

-- 28. SUPPORT_AGENT_METRICS
CREATE OR REPLACE TABLE support_agent_metrics (
    metric_id VARCHAR(50) PRIMARY KEY,
    agent_name VARCHAR(100),
    metric_date DATE,
    tickets_opened INTEGER,
    tickets_closed INTEGER,
    avg_resolution_time_hrs NUMBER(10,2),
    first_response_time_avg NUMBER(10,2),
    csat_score_avg NUMBER(5,2),
    sla_breach_count INTEGER,
    escalation_count INTEGER,
    reopened_tickets_count INTEGER,
    utilization_percent NUMBER(5,2)
);

-- 51. SUPPORT_CHANNELS
CREATE OR REPLACE TABLE support_channels (
    channel_id VARCHAR(50) PRIMARY KEY,
    channel_name VARCHAR(100),
    active VARCHAR(10),
    hours_of_operation VARCHAR(200),
    avg_response_time_min NUMBER(10,2),
    satisfaction_score NUMBER(5,2),
    volume_percent NUMBER(5,2),
    cost_per_ticket NUMBER(10,2)
);

-- 52. TICKET_WORKLOG
CREATE OR REPLACE TABLE ticket_worklog (
    worklog_id VARCHAR(50) PRIMARY KEY,
    ticket_id VARCHAR(50),
    agent_name VARCHAR(100),
    work_date DATE,
    time_spent_minutes INTEGER,
    work_description TEXT,
    billable VARCHAR(10),
    internal_only VARCHAR(10)
);

-- ============================================================================
-- OPERATIONAL_DATA SCHEMA - 10 Tables
-- ============================================================================

USE SCHEMA sales_analytics_demo.operational_data;

-- 53. CUSTOMER_HEALTH_SCORE
CREATE OR REPLACE TABLE customer_health_score (
    health_score_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    score_date DATE,
    overall_score NUMBER(5,2),
    product_usage_score NUMBER(5,2),
    support_health_score NUMBER(5,2),
    payment_health_score NUMBER(5,2),
    engagement_score NUMBER(5,2),
    sentiment_score NUMBER(5,2),
    risk_level VARCHAR(50),
    churn_probability NUMBER(5,2),
    expansion_probability NUMBER(5,2),
    health_trend VARCHAR(50)
);

-- 54. ONBOARDING_PROGRESS
CREATE OR REPLACE TABLE onboarding_progress (
    onboarding_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    start_date DATE,
    target_completion_date DATE,
    actual_completion_date DATE,
    status VARCHAR(50),
    progress_percent NUMBER(5,2),
    milestone_count INTEGER,
    milestones_completed INTEGER,
    csm_assigned VARCHAR(100),
    health_status VARCHAR(50),
    days_to_first_value INTEGER,
    adoption_rate NUMBER(5,2)
);

-- 55. PRODUCT_RELEASES
CREATE OR REPLACE TABLE product_releases (
    release_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50),
    version_number VARCHAR(50),
    release_date DATE,
    release_type VARCHAR(50),
    features_added INTEGER,
    bugs_fixed INTEGER,
    breaking_changes VARCHAR(10),
    eol_date DATE,
    support_end_date DATE,
    adoption_rate_percent NUMBER(5,2),
    release_notes_url VARCHAR(500)
);

-- 56. CUSTOMER_SUCCESS_ACTIVITIES
CREATE OR REPLACE TABLE customer_success_activities (
    activity_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    activity_date DATE,
    activity_type VARCHAR(100),
    csm_name VARCHAR(100),
    outcome TEXT,
    sentiment VARCHAR(50),
    action_items TEXT,
    next_activity_date DATE,
    attendees VARCHAR(500),
    duration_minutes INTEGER
);

-- 57. USER_ACCOUNTS
CREATE OR REPLACE TABLE user_accounts (
    user_account_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    username VARCHAR(200),
    email VARCHAR(200),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    role VARCHAR(50),
    status VARCHAR(50),
    created_date DATE,
    last_login_date DATE,
    login_count INTEGER,
    password_reset_date DATE,
    mfa_enabled VARCHAR(10)
);

-- 58. AUDIT_LOG
CREATE OR REPLACE TABLE audit_log (
    audit_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    user_account_id VARCHAR(50),
    event_timestamp TIMESTAMP,
    event_type VARCHAR(100),
    entity_type VARCHAR(100),
    entity_id VARCHAR(100),
    action_description VARCHAR(500),
    ip_address VARCHAR(50),
    success VARCHAR(10),
    error_message VARCHAR(500)
);

-- 59. NOTIFICATIONS
CREATE OR REPLACE TABLE notifications (
    notification_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    user_account_id VARCHAR(50),
    notification_type VARCHAR(100),
    priority VARCHAR(50),
    subject VARCHAR(500),
    message TEXT,
    sent_date DATE,
    read_date DATE,
    status VARCHAR(50),
    channel VARCHAR(50),
    related_entity_type VARCHAR(100),
    related_entity_id VARCHAR(100)
);

-- 60. INVOICES
CREATE OR REPLACE TABLE invoices (
    invoice_id VARCHAR(50) PRIMARY KEY,
    invoice_number VARCHAR(50),
    customer_id VARCHAR(50),
    contract_id VARCHAR(50),
    invoice_date DATE,
    due_date DATE,
    total_amount NUMBER(15,2),
    tax_amount NUMBER(15,2),
    subtotal NUMBER(15,2),
    status VARCHAR(50),
    payment_method VARCHAR(50),
    payment_date DATE,
    payment_reference VARCHAR(100),
    currency VARCHAR(10),
    billing_period_start DATE,
    billing_period_end DATE
);

-- 61. PAYMENTS
CREATE OR REPLACE TABLE payments (
    payment_id VARCHAR(50) PRIMARY KEY,
    invoice_id VARCHAR(50),
    customer_id VARCHAR(50),
    payment_date DATE,
    payment_amount NUMBER(15,2),
    payment_method VARCHAR(50),
    transaction_id VARCHAR(100),
    status VARCHAR(50),
    currency VARCHAR(10),
    processing_fee NUMBER(15,2),
    net_amount NUMBER(15,2),
    payment_gateway VARCHAR(100)
);

-- 62. RENEWALS
CREATE OR REPLACE TABLE renewals (
    renewal_id VARCHAR(50) PRIMARY KEY,
    contract_id VARCHAR(50),
    customer_id VARCHAR(50),
    current_expiration_date DATE,
    renewal_date DATE,
    renewal_amount NUMBER(15,2),
    renewal_type VARCHAR(50),
    renewal_status VARCHAR(50),
    probability_percent INTEGER,
    owner VARCHAR(100),
    last_contact_date DATE,
    next_contact_date DATE,
    churn_risk VARCHAR(50),
    churn_reason VARCHAR(200),
    notes TEXT
);

-- ============================================================================
-- STEP 4: Load Data from S3/Stage
-- ============================================================================

-- NOTE: Run these COPY commands after uploading CSV files to your stage
-- Replace @s3_stage with your actual stage name

-- SALES_DATA
USE SCHEMA sales_analytics_demo.sales_data;

COPY INTO customers FROM @sales_analytics_demo.public.csv_stage/sales_data/customers.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO products FROM @sales_analytics_demo.public.csv_stage/sales_data/products.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO customer_products FROM @sales_analytics_demo.public.csv_stage/sales_data/customer_products.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO opportunities FROM @sales_analytics_demo.public.csv_stage/sales_data/opportunities.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO closed_deals FROM @sales_analytics_demo.public.csv_stage/sales_data/closed_deals.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO sales_targets FROM @sales_analytics_demo.public.csv_stage/sales_data/sales_targets.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO customer_attributes FROM @sales_analytics_demo.public.csv_stage/sales_data/customer_attributes.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO customer_contacts FROM @sales_analytics_demo.public.csv_stage/sales_data/customer_contacts.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO account_hierarchy FROM @sales_analytics_demo.public.csv_stage/sales_data/account_hierarchy.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO customer_notes FROM @sales_analytics_demo.public.csv_stage/sales_data/customer_notes.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO competitive_intelligence FROM @sales_analytics_demo.public.csv_stage/sales_data/competitive_intelligence.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO quotes FROM @sales_analytics_demo.public.csv_stage/sales_data/quotes.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO quote_line_items FROM @sales_analytics_demo.public.csv_stage/sales_data/quote_line_items.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO contracts FROM @sales_analytics_demo.public.csv_stage/sales_data/contracts.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO revenue_recognition FROM @sales_analytics_demo.public.csv_stage/sales_data/revenue_recognition.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO territories FROM @sales_analytics_demo.public.csv_stage/sales_data/territories.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';

-- USAGE_DATA
USE SCHEMA sales_analytics_demo.usage_data;

COPY INTO product_usage_summary FROM @sales_analytics_demo.public.csv_stage/usage_data/product_usage_summary.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO feature_usage FROM @sales_analytics_demo.public.csv_stage/usage_data/feature_usage.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO telemetry_events FROM @sales_analytics_demo.public.csv_stage/usage_data/telemetry_events.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO product_health_alerts FROM @sales_analytics_demo.public.csv_stage/usage_data/product_health_alerts.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO user_sessions FROM @sales_analytics_demo.public.csv_stage/usage_data/user_sessions.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO api_usage FROM @sales_analytics_demo.public.csv_stage/usage_data/api_usage.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO integration_usage FROM @sales_analytics_demo.public.csv_stage/usage_data/integration_usage.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO license_usage FROM @sales_analytics_demo.public.csv_stage/usage_data/license_usage.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO data_quality_metrics FROM @sales_analytics_demo.public.csv_stage/usage_data/data_quality_metrics.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO performance_metrics FROM @sales_analytics_demo.public.csv_stage/usage_data/performance_metrics.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';

-- MARKETING_DATA
USE SCHEMA sales_analytics_demo.marketing_data;

COPY INTO campaigns FROM @sales_analytics_demo.public.csv_stage/marketing_data/campaigns.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO events FROM @sales_analytics_demo.public.csv_stage/marketing_data/events.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO leads FROM @sales_analytics_demo.public.csv_stage/marketing_data/leads.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO campaign_engagement FROM @sales_analytics_demo.public.csv_stage/marketing_data/campaign_engagement.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO marketing_attribution FROM @sales_analytics_demo.public.csv_stage/marketing_data/marketing_attribution.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO webinar_attendance FROM @sales_analytics_demo.public.csv_stage/marketing_data/webinar_attendance.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO content_assets FROM @sales_analytics_demo.public.csv_stage/marketing_data/content_assets.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO email_campaigns FROM @sales_analytics_demo.public.csv_stage/marketing_data/email_campaigns.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO social_media_engagement FROM @sales_analytics_demo.public.csv_stage/marketing_data/social_media_engagement.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO website_traffic FROM @sales_analytics_demo.public.csv_stage/marketing_data/website_traffic.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO partner_referrals FROM @sales_analytics_demo.public.csv_stage/marketing_data/partner_referrals.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO marketing_budget FROM @sales_analytics_demo.public.csv_stage/marketing_data/marketing_budget.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO lead_scoring_rules FROM @sales_analytics_demo.public.csv_stage/marketing_data/lead_scoring_rules.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';

-- SUPPORT_DATA
USE SCHEMA sales_analytics_demo.support_data;

COPY INTO support_tickets FROM @sales_analytics_demo.public.csv_stage/support_data/support_tickets.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO ticket_comments FROM @sales_analytics_demo.public.csv_stage/support_data/ticket_comments.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO ticket_resolution FROM @sales_analytics_demo.public.csv_stage/support_data/ticket_resolution.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO customer_satisfaction FROM @sales_analytics_demo.public.csv_stage/support_data/customer_satisfaction.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO sla_policies FROM @sales_analytics_demo.public.csv_stage/support_data/sla_policies.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO sla_breaches FROM @sales_analytics_demo.public.csv_stage/support_data/sla_breaches.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO knowledge_base FROM @sales_analytics_demo.public.csv_stage/support_data/knowledge_base.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO ticket_tags FROM @sales_analytics_demo.public.csv_stage/support_data/ticket_tags.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO escalations FROM @sales_analytics_demo.public.csv_stage/support_data/escalations.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO recurring_issues FROM @sales_analytics_demo.public.csv_stage/support_data/recurring_issues.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO support_agent_metrics FROM @sales_analytics_demo.public.csv_stage/support_data/support_agent_metrics.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO support_channels FROM @sales_analytics_demo.public.csv_stage/support_data/support_channels.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO ticket_worklog FROM @sales_analytics_demo.public.csv_stage/support_data/ticket_worklog.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';

-- OPERATIONAL_DATA
USE SCHEMA sales_analytics_demo.operational_data;

COPY INTO customer_health_score FROM @sales_analytics_demo.public.csv_stage/operational_data/customer_health_score.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO onboarding_progress FROM @sales_analytics_demo.public.csv_stage/operational_data/onboarding_progress.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO product_releases FROM @sales_analytics_demo.public.csv_stage/operational_data/product_releases.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO customer_success_activities FROM @sales_analytics_demo.public.csv_stage/operational_data/customer_success_activities.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO user_accounts FROM @sales_analytics_demo.public.csv_stage/operational_data/user_accounts.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO audit_log FROM @sales_analytics_demo.public.csv_stage/operational_data/audit_log.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO notifications FROM @sales_analytics_demo.public.csv_stage/operational_data/notifications.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO invoices FROM @sales_analytics_demo.public.csv_stage/operational_data/invoices.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO payments FROM @sales_analytics_demo.public.csv_stage/operational_data/payments.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';
COPY INTO renewals FROM @sales_analytics_demo.public.csv_stage/operational_data/renewals.csv FILE_FORMAT = (FORMAT_NAME = 'sales_analytics_demo.public.csv_format') ON_ERROR = 'CONTINUE';

-- ============================================================================
-- STEP 5: Verify Data Load
-- ============================================================================

-- Check record counts
USE SCHEMA sales_analytics_demo.sales_data;
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'opportunities', COUNT(*) FROM opportunities
UNION ALL SELECT 'closed_deals', COUNT(*) FROM closed_deals
UNION ALL SELECT 'customer_products', COUNT(*) FROM customer_products;

USE SCHEMA sales_analytics_demo.marketing_data;
SELECT 'campaigns' AS table_name, COUNT(*) AS row_count FROM campaigns
UNION ALL SELECT 'leads', COUNT(*) FROM leads
UNION ALL SELECT 'events', COUNT(*) FROM events;

USE SCHEMA sales_analytics_demo.support_data;
SELECT 'support_tickets' AS table_name, COUNT(*) AS row_count FROM support_tickets
UNION ALL SELECT 'customer_satisfaction', COUNT(*) FROM customer_satisfaction;

USE SCHEMA sales_analytics_demo.usage_data;
SELECT 'product_usage_summary' AS table_name, COUNT(*) AS row_count FROM product_usage_summary
UNION ALL SELECT 'api_usage', COUNT(*) FROM api_usage;

USE SCHEMA sales_analytics_demo.operational_data;
SELECT 'customer_health_score' AS table_name, COUNT(*) AS row_count FROM customer_health_score
UNION ALL SELECT 'invoices', COUNT(*) FROM invoices
UNION ALL SELECT 'payments', COUNT(*) FROM payments;

-- ============================================================================
-- STEP 6: Create Useful Views (Optional)
-- ============================================================================

USE SCHEMA sales_analytics_demo.sales_data;

-- Customer 360 View
CREATE OR REPLACE VIEW customer_360 AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.industry,
    c.company_size,
    c.region,
    c.customer_tier,
    c.account_owner,
    COUNT(DISTINCT cp.product_id) as product_count,
    SUM(cp.contract_value) as total_contract_value,
    COUNT(DISTINCT o.opportunity_id) as opportunity_count,
    COUNT(DISTINCT CASE WHEN cd.status = 'Won' THEN cd.deal_id END) as won_deals,
    COUNT(DISTINCT CASE WHEN cd.status = 'Lost' THEN cd.deal_id END) as lost_deals,
    MAX(h.overall_score) as latest_health_score,
    MAX(h.churn_probability) as churn_risk
FROM customers c
LEFT JOIN customer_products cp ON c.customer_id = cp.customer_id
LEFT JOIN opportunities o ON c.customer_id = o.customer_id
LEFT JOIN closed_deals cd ON o.opportunity_id = cd.opportunity_id
LEFT JOIN sales_analytics_demo.operational_data.customer_health_score h 
    ON c.customer_id = h.customer_id
GROUP BY c.customer_id, c.customer_name, c.industry, c.company_size, 
         c.region, c.customer_tier, c.account_owner;

-- Sales Performance Dashboard View
CREATE OR REPLACE VIEW sales_performance_summary AS
SELECT 
    cd.quarter,
    cd.fiscal_year,
    cd.region,
    cd.status,
    COUNT(*) as deal_count,
    SUM(cd.deal_value) as total_revenue,
    AVG(cd.deal_value) as avg_deal_size,
    AVG(cd.sales_cycle_days) as avg_sales_cycle,
    SUM(CASE WHEN cd.discount_percent > 0 THEN 1 ELSE 0 END) as discounted_deals,
    AVG(cd.discount_percent) as avg_discount
FROM closed_deals cd
GROUP BY cd.quarter, cd.fiscal_year, cd.region, cd.status;

-- Pipeline Health View
CREATE OR REPLACE VIEW pipeline_health AS
SELECT 
    o.sales_rep,
    o.stage,
    COUNT(*) as opp_count,
    SUM(o.amount) as pipeline_value,
    AVG(o.probability) as avg_probability,
    SUM(o.amount * o.probability / 100) as weighted_pipeline,
    AVG(DATEDIFF(day, o.created_date, CURRENT_DATE())) as avg_age_days
FROM opportunities o
WHERE o.stage NOT IN ('Closed Won', 'Closed Lost')
GROUP BY o.sales_rep, o.stage;

-- Support Metrics View
USE SCHEMA sales_analytics_demo.support_data;

CREATE OR REPLACE VIEW support_metrics_summary AS
SELECT 
    DATE_TRUNC('month', st.created_date) as month,
    COUNT(*) as total_tickets,
    COUNT(CASE WHEN st.status IN ('Resolved', 'Closed') THEN 1 END) as resolved_tickets,
    AVG(CASE WHEN st.resolved_date IS NOT NULL 
        THEN DATEDIFF(hour, st.created_date, st.resolved_date) END) as avg_resolution_hours,
    AVG(CASE WHEN st.first_response_date IS NOT NULL 
        THEN DATEDIFF(hour, st.created_date, st.first_response_date) END) as avg_first_response_hours,
    COUNT(CASE WHEN st.sla_status = 'Met' THEN 1 END) as sla_met,
    COUNT(CASE WHEN st.sla_status = 'Breached' THEN 1 END) as sla_breached,
    AVG(csat.satisfaction_score) as avg_csat
FROM support_tickets st
LEFT JOIN customer_satisfaction csat ON st.ticket_id = csat.ticket_id
GROUP BY DATE_TRUNC('month', st.created_date);

-- ============================================================================
-- STEP 7: Grant Permissions (Optional)
-- ============================================================================

-- Grant read access to a role
-- GRANT USAGE ON DATABASE sales_analytics_demo TO ROLE analyst_role;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE sales_analytics_demo TO ROLE analyst_role;
-- GRANT SELECT ON ALL TABLES IN DATABASE sales_analytics_demo TO ROLE analyst_role;
-- GRANT SELECT ON ALL VIEWS IN DATABASE sales_analytics_demo TO ROLE analyst_role;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

SELECT '✅ All 62 tables created successfully!' AS status,
       'Run the COPY commands to load data from your S3 stage' AS next_step;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================