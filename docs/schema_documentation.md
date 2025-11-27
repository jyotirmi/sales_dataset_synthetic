# Sales Analytics Demo - Complete Schema Documentation

## 📚 Table of Contents

- [Sales & Customer Data (17 tables)](#sales--customer-data)
- [Product Usage & Telemetry (11 tables)](#product-usage--telemetry)
- [Marketing Data (13 tables)](#marketing-data)
- [Support & Service (12 tables)](#support--service)
- [Operational Data (9 tables)](#operational-data)
- [Relationships & Foreign Keys](#relationships--foreign-keys)

---

# Sales & Customer Data

## 1. CUSTOMERS
**Purpose**: Core customer/account information  
**Schema**: `sales_data`  
**Estimated Records**: 1,000

| Column | Type | Description |
|--------|------|-------------|
| customer_id | VARCHAR(50) PK | Unique customer identifier (CUST-XXXXXX) |
| customer_name | VARCHAR(200) | Company name |
| industry | VARCHAR(100) | Industry sector (Technology, Healthcare, Finance, etc.) |
| company_size | VARCHAR(50) | Small, Medium, Large, Enterprise |
| region | VARCHAR(100) | Geographic region (North America, EMEA, APAC, Latin America) |
| country | VARCHAR(100) | Specific country |
| annual_revenue | NUMBER(15,2) | Company annual revenue |
| account_owner | VARCHAR(100) | Assigned sales representative |
| created_date | DATE | When customer was acquired |
| customer_tier | VARCHAR(50) | Bronze, Silver, Gold, Platinum |

**Sample Use Cases**:
- Customer segmentation analysis
- Territory planning
- Account prioritization

---

## 2. PRODUCTS
**Purpose**: Product catalog and pricing  
**Schema**: `sales_data`  
**Estimated Records**: 18

| Column | Type | Description |
|--------|------|-------------|
| product_id | VARCHAR(50) PK | Unique product identifier (PROD-XXXXXX) |
| product_name | VARCHAR(200) | Product name |
| product_category | VARCHAR(100) | Analytics, Security, Collaboration, Integration, Storage |
| current_version | VARCHAR(50) | Latest version number |
| unit_price | NUMBER(15,2) | Base price per unit |
| product_line | VARCHAR(50) | Core, Premium, Enterprise |
| release_date | DATE | Product launch date |

**Sample Use Cases**:
- Product portfolio analysis
- Pricing optimization
- Version adoption tracking

---

## 3. CUSTOMER_PRODUCTS
**Purpose**: Customer install base - which customers have which products  
**Schema**: `sales_data`  
**Estimated Records**: 2,000

| Column | Type | Description |
|--------|------|-------------|
| install_id | VARCHAR(50) PK | Unique installation identifier (INST-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| version_number | VARCHAR(50) | Product version installed |
| license_count | INTEGER | Number of licenses |
| install_date | DATE | Installation date |
| contract_value | NUMBER(15,2) | Annual contract value for this product |
| renewal_date | DATE | Next renewal date |
| usage_tier | VARCHAR(50) | Low, Medium, High usage level |
| support_level | VARCHAR(50) | Basic, Standard, Premium support tier |

**Sample Use Cases**:
- Install base analysis
- Cross-sell/upsell identification
- Version migration planning
- Contract renewal tracking

---

## 4. OPPORTUNITIES
**Purpose**: Sales pipeline and opportunity tracking  
**Schema**: `sales_data`  
**Estimated Records**: 4,000

| Column | Type | Description |
|--------|------|-------------|
| opportunity_id | VARCHAR(50) PK | Unique opportunity identifier (OPP-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| opportunity_name | VARCHAR(500) | Deal name |
| stage | VARCHAR(100) | Prospecting, Qualification, Proposal, Negotiation, Closed Won, Closed Lost |
| amount | NUMBER(15,2) | Deal value |
| probability | INTEGER | Win probability (0-100) |
| close_date | DATE | Expected/actual close date |
| created_date | DATE | Opportunity creation date |
| opportunity_type | VARCHAR(100) | New Business, Upsell, Cross-sell, Renewal |
| region | VARCHAR(100) | Geographic region |
| sales_rep | VARCHAR(100) | Opportunity owner |
| lead_source | VARCHAR(100) | Marketing, Sales, Partner, Web, Referral |
| competitor | VARCHAR(200) | Primary competitor (if any) |

**Sample Use Cases**:
- Pipeline analysis and forecasting
- Sales stage conversion rates
- Lead source effectiveness
- Competitive win/loss analysis

---

## 5. CLOSED_DEALS
**Purpose**: Historical closed opportunities (won and lost)  
**Schema**: `sales_data`  
**Estimated Records**: 2,500

| Column | Type | Description |
|--------|------|-------------|
| deal_id | VARCHAR(50) PK | Unique deal identifier (DEAL-XXXXXX) |
| opportunity_id | VARCHAR(50) FK | References OPPORTUNITIES |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| deal_value | NUMBER(15,2) | Final closed amount (0 if lost) |
| close_date | DATE | Date deal was closed |
| quarter | VARCHAR(20) | Fiscal quarter (Q1 2024, Q2 2024, etc.) |
| fiscal_year | INTEGER | Fiscal year |
| status | VARCHAR(20) | Won or Lost |
| win_loss_reason | VARCHAR(200) | Reason for outcome |
| sales_cycle_days | INTEGER | Days from creation to close |
| discount_percent | NUMBER(5,2) | Discount given (if won) |
| region | VARCHAR(100) | Geographic region |
| sales_rep | VARCHAR(100) | Deal owner |

**Sample Use Cases**:
- Win/loss analysis
- Sales cycle optimization
- Quota attainment tracking
- Deal velocity analysis
- Discount impact analysis

---

## 6. SALES_TARGETS
**Purpose**: Quarterly sales quotas by region  
**Schema**: `sales_data`  
**Estimated Records**: 100

| Column | Type | Description |
|--------|------|-------------|
| target_id | VARCHAR(50) PK | Unique target identifier (TGT-XXXXXX) |
| quarter | VARCHAR(20) | Q1 2024, Q2 2024, etc. |
| region | VARCHAR(100) | Geographic region |
| product_id | VARCHAR(50) FK | Product-specific target (optional) |
| target_amount | NUMBER(15,2) | Quota amount |
| sales_rep | VARCHAR(100) | Rep-specific target (optional) |

**Sample Use Cases**:
- Quota vs actuals comparison
- Sales performance tracking
- Territory performance analysis

---

## 7. CUSTOMER_ATTRIBUTES
**Purpose**: Additional customer metadata for segmentation  
**Schema**: `sales_data`  
**Estimated Records**: 3,000

| Column | Type | Description |
|--------|------|-------------|
| attribute_id | VARCHAR(50) PK | Unique attribute identifier (ATTR-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| attribute_type | VARCHAR(100) | Certification, Technology Stack, Use Case, Integration, Compliance |
| attribute_value | VARCHAR(200) | Specific value (ISO 9001, AWS, HIPAA, etc.) |
| created_date | DATE | When attribute was added |

**Sample Use Cases**:
- Customer segmentation
- Compliance tracking
- Technology stack analysis
- Use case identification

---

## 29. CUSTOMER_CONTACTS
**Purpose**: Individual contacts at customer accounts  
**Schema**: `sales_data`  
**Estimated Records**: 4,000

| Column | Type | Description |
|--------|------|-------------|
| contact_id | VARCHAR(50) PK | Unique contact identifier (CONT-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| first_name | VARCHAR(100) | Contact first name |
| last_name | VARCHAR(100) | Contact last name |
| email | VARCHAR(200) | Email address |
| phone | VARCHAR(50) | Phone number |
| title | VARCHAR(100) | Job title |
| department | VARCHAR(100) | Department (IT, Engineering, Finance, etc.) |
| role_type | VARCHAR(50) | Decision Maker, Influencer, Champion, Blocker, User |
| is_primary | VARCHAR(10) | Primary contact flag (Yes/No) |
| linkedin_url | VARCHAR(500) | LinkedIn profile URL |
| last_contact_date | DATE | Last interaction date |
| created_date | DATE | When contact was added |
| status | VARCHAR(50) | Active, Inactive, Bounced |

**Sample Use Cases**:
- Contact relationship mapping
- Decision maker identification
- Multi-threading analysis
- Stakeholder engagement tracking

---

## 30. ACCOUNT_HIERARCHY
**Purpose**: Parent-child relationships between customer accounts  
**Schema**: `sales_data`  
**Estimated Records**: 200

| Column | Type | Description |
|--------|------|-------------|
| hierarchy_id | VARCHAR(50) PK | Unique hierarchy identifier (HIER-XXXXXX) |
| parent_customer_id | VARCHAR(50) FK | Parent company |
| child_customer_id | VARCHAR(50) FK | Subsidiary/division |
| relationship_type | VARCHAR(100) | Parent-Child, Sister Company, Division, Franchise |
| ownership_percent | NUMBER(5,2) | Ownership percentage (optional) |
| created_date | DATE | When relationship was established |

**Sample Use Cases**:
- Enterprise account mapping
- Strategic account planning
- Deal consolidation
- Corporate structure analysis

---

## 31. CUSTOMER_NOTES
**Purpose**: Sales and account activity notes  
**Schema**: `sales_data`  
**Estimated Records**: 5,000

| Column | Type | Description |
|--------|------|-------------|
| note_id | VARCHAR(50) PK | Unique note identifier (NOTE-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| contact_id | VARCHAR(50) FK | References CUSTOMER_CONTACTS (optional) |
| note_date | DATE | Note creation date |
| note_type | VARCHAR(50) | Meeting, Call, Email, Internal, Other |
| created_by | VARCHAR(100) | Note author |
| subject | VARCHAR(500) | Note subject |
| note_text | TEXT | Full note content |
| opportunity_id | VARCHAR(50) FK | Related opportunity (optional) |
| next_action | VARCHAR(200) | Follow-up action needed |
| next_action_date | DATE | Follow-up due date |

**Sample Use Cases**:
- Activity tracking
- Customer interaction history
- Follow-up management
- Sales coaching

---

## 32. COMPETITIVE_INTELLIGENCE
**Purpose**: Competitive landscape and intel gathering  
**Schema**: `sales_data`  
**Estimated Records**: 2,000

| Column | Type | Description |
|--------|------|-------------|
| intel_id | VARCHAR(50) PK | Unique intel identifier (INTEL-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| opportunity_id | VARCHAR(50) FK | References OPPORTUNITIES (optional) |
| competitor_name | VARCHAR(200) | Competitor name |
| competitor_product | VARCHAR(200) | Their product |
| intel_date | DATE | When intelligence was gathered |
| intel_source | VARCHAR(100) | Sales call, Customer, Market research, Partner, etc. |
| competitor_strength | VARCHAR(200) | What they're good at |
| competitor_weakness | VARCHAR(200) | What they're weak at |
| our_position | VARCHAR(50) | Winning, At Risk, Lost |
| intel_type | VARCHAR(100) | Product comparison, Pricing, Customer feedback |
| notes | TEXT | Additional details |

**Sample Use Cases**:
- Competitive analysis
- Battle card creation
- Win/loss pattern identification
- Product positioning

---

## 33. QUOTES
**Purpose**: Price quotes generated for opportunities  
**Schema**: `sales_data`  
**Estimated Records**: 3,000

| Column | Type | Description |
|--------|------|-------------|
| quote_id | VARCHAR(50) PK | Unique quote identifier (QUOT-XXXXXX) |
| quote_number | VARCHAR(50) | Human-readable quote number |
| opportunity_id | VARCHAR(50) FK | References OPPORTUNITIES |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| quote_date | DATE | Quote creation date |
| expiration_date | DATE | Quote expiration date |
| total_amount | NUMBER(15,2) | Total quote value |
| discount_percent | NUMBER(5,2) | Overall discount applied |
| payment_terms | VARCHAR(50) | Net 30, Net 60, etc. |
| delivery_terms | VARCHAR(100) | Shipping/delivery terms |
| status | VARCHAR(50) | Draft, Sent, Accepted, Rejected, Expired |
| created_by | VARCHAR(100) | Sales rep who created |
| approved_by | VARCHAR(100) | Manager who approved |
| approval_date | DATE | Approval date |
| notes | TEXT | Additional notes |

**Sample Use Cases**:
- Quote-to-close conversion
- Pricing analysis
- Discount effectiveness
- Quote velocity tracking

---

## 34. QUOTE_LINE_ITEMS
**Purpose**: Individual line items within quotes  
**Schema**: `sales_data`  
**Estimated Records**: 8,000

| Column | Type | Description |
|--------|------|-------------|
| line_item_id | VARCHAR(50) PK | Unique line item identifier (LINE-XXXXXX) |
| quote_id | VARCHAR(50) FK | References QUOTES |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| quantity | INTEGER | Number of units |
| unit_price | NUMBER(15,2) | Price per unit |
| discount_percent | NUMBER(5,2) | Line item discount |
| line_total | NUMBER(15,2) | Total for this line |
| description | VARCHAR(500) | Item description |
| product_version | VARCHAR(50) | Version quoted |
| license_type | VARCHAR(50) | Perpetual, Subscription, Usage-based |
| term_months | INTEGER | Contract length in months |

**Sample Use Cases**:
- Product mix analysis
- Bundle optimization
- Pricing strategy
- Average deal composition

---

## 35. CONTRACTS
**Purpose**: Executed customer contracts  
**Schema**: `sales_data`  
**Estimated Records**: 1,500

| Column | Type | Description |
|--------|------|-------------|
| contract_id | VARCHAR(50) PK | Unique contract identifier (CONT-XXXXXX) |
| contract_number | VARCHAR(50) | Human-readable contract number |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| opportunity_id | VARCHAR(50) FK | References OPPORTUNITIES |
| quote_id | VARCHAR(50) FK | References QUOTES |
| contract_type | VARCHAR(100) | Master Agreement, SOW, Amendment, Renewal |
| start_date | DATE | Contract start date |
| end_date | DATE | Contract end date |
| contract_value | NUMBER(15,2) | Total contract value |
| annual_value | NUMBER(15,2) | ARR (Annual Recurring Revenue) |
| payment_schedule | VARCHAR(50) | Monthly, Quarterly, Annual, Upfront |
| auto_renew | VARCHAR(10) | Yes/No |
| renewal_notice_days | INTEGER | Days notice required for renewal |
| status | VARCHAR(50) | Draft, Active, Expired, Terminated |
| signed_date | DATE | Date signed |
| signed_by_customer | VARCHAR(200) | Customer signatory |
| signed_by_vendor | VARCHAR(200) | Vendor signatory |

**Sample Use Cases**:
- Contract management
- Renewal pipeline
- Revenue recognition
- Compliance tracking

---

## 36. REVENUE_RECOGNITION
**Purpose**: Revenue recognition schedule for accounting  
**Schema**: `sales_data`  
**Estimated Records**: 6,000

| Column | Type | Description |
|--------|------|-------------|
| recognition_id | VARCHAR(50) PK | Unique recognition identifier |
| contract_id | VARCHAR(50) FK | References CONTRACTS |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| recognition_date | DATE | Date of revenue recognition |
| recognition_amount | NUMBER(15,2) | Amount recognized |
| recognition_type | VARCHAR(50) | Monthly, Quarterly, Milestone |
| fiscal_quarter | VARCHAR(20) | Q1 2024, etc. |
| fiscal_year | INTEGER | Fiscal year |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| region | VARCHAR(100) | Geographic region |
| deferred_revenue | NUMBER(15,2) | Amount deferred |
| recognized_to_date | NUMBER(15,2) | Cumulative recognized amount |

**Sample Use Cases**:
- Financial reporting
- Revenue forecasting
- Deferred revenue tracking
- ARR/MRR analysis

---

## 37. TERRITORIES
**Purpose**: Sales territory definitions and assignments  
**Schema**: `sales_data`  
**Estimated Records**: 50

| Column | Type | Description |
|--------|------|-------------|
| territory_id | VARCHAR(50) PK | Unique territory identifier |
| territory_name | VARCHAR(200) | Territory name |
| region | VARCHAR(100) | Parent region |
| country_list | VARCHAR(1000) | Countries in territory |
| territory_type | VARCHAR(100) | Geographic, Named Accounts, Industry |
| sales_rep | VARCHAR(100) | Assigned sales rep |
| manager | VARCHAR(100) | Territory manager |
| quota | NUMBER(15,2) | Annual quota for territory |
| active | VARCHAR(10) | Yes/No |
| effective_date | DATE | When territory became active |

**Sample Use Cases**:
- Territory planning
- Quota allocation
- Coverage analysis
- Rep performance by territory

---

# Product Usage & Telemetry

## 8. PRODUCT_USAGE_SUMMARY
**Purpose**: Aggregated daily/weekly product usage metrics  
**Schema**: `usage_data`  
**Estimated Records**: 20,000

| Column | Type | Description |
|--------|------|-------------|
| usage_id | VARCHAR(50) PK | Unique usage record identifier (USG-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| usage_date | DATE | Date of usage measurement |
| active_users | INTEGER | Number of active users |
| total_sessions | INTEGER | Total sessions |
| avg_session_duration_min | NUMBER(10,2) | Average session length in minutes |
| feature_adoption_score | NUMBER(5,2) | Feature adoption score (0-100) |
| data_volume_gb | NUMBER(15,2) | Data processed/stored in GB |
| api_calls | INTEGER | Number of API calls |
| error_rate_percent | NUMBER(5,2) | Error rate percentage |
| login_count | INTEGER | Number of logins |
| license_utilization | NUMBER(5,2) | Percentage of licenses in use |
| health_score | VARCHAR(50) | Healthy, At Risk, Critical |

**Sample Use Cases**:
- Usage trend analysis
- License optimization
- Customer health monitoring
- Feature adoption tracking

---

## 9. FEATURE_USAGE
**Purpose**: Granular feature-level usage tracking  
**Schema**: `usage_data`  
**Estimated Records**: 50,000

| Column | Type | Description |
|--------|------|-------------|
| feature_usage_id | VARCHAR(50) PK | Unique feature usage identifier (FEAT-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| feature_name | VARCHAR(200) | Specific feature name |
| usage_date | DATE | Date of usage |
| usage_count | INTEGER | Times feature was used |
| unique_users | INTEGER | Number of unique users |
| adoption_status | VARCHAR(50) | Not Adopted, Exploring, Regular, Power User |

**Sample Use Cases**:
- Feature popularity analysis
- Adoption curve tracking
- Product roadmap prioritization
- User behavior analysis

---

## 10. TELEMETRY_EVENTS
**Purpose**: Raw event stream from product telemetry  
**Schema**: `usage_data`  
**Estimated Records**: 100,000

| Column | Type | Description |
|--------|------|-------------|
| event_id | VARCHAR(50) PK | Unique event identifier (TELEM-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| event_timestamp | TIMESTAMP | Exact time of event |
| event_type | VARCHAR(100) | Login, Error, Feature_Used, Export, Integration, etc. |
| event_category | VARCHAR(100) | Usage, Performance, Security, Integration |
| severity | VARCHAR(50) | Info, Warning, Error, Critical |
| user_id | VARCHAR(100) | End user identifier |
| session_id | VARCHAR(100) | Session identifier |
| metadata_json | VARIANT/JSON | Additional event data in JSON format |

**Sample Use Cases**:
- Error tracking and debugging
- Security monitoring
- Performance analysis
- User journey mapping

---

## 11. PRODUCT_HEALTH_ALERTS
**Purpose**: Automated alerts for product health issues  
**Schema**: `usage_data`  
**Estimated Records**: 1,000

| Column | Type | Description |
|--------|------|-------------|
| alert_id | VARCHAR(50) PK | Unique alert identifier (ALERT-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| alert_date | DATE | When alert was triggered |
| alert_type | VARCHAR(200) | Low Usage, High Error Rate, License Expiring, Version Outdated, etc. |
| severity | VARCHAR(50) | Low, Medium, High, Critical |
| status | VARCHAR(50) | Open, Acknowledged, Resolved |
| assigned_to | VARCHAR(100) | CSM or support rep |
| resolution_date | DATE | When alert was resolved |

**Sample Use Cases**:
- Proactive customer success
- Churn prevention
- Health score calculation
- CSM workload management

---

## 38. USER_SESSIONS
**Purpose**: Detailed individual user session data  
**Schema**: `usage_data`  
**Estimated Records**: 50,000

| Column | Type | Description |
|--------|------|-------------|
| session_id | VARCHAR(50) PK | Unique session identifier (SESS-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| user_id | VARCHAR(100) | End user identifier |
| session_start | TIMESTAMP | Session start time |
| session_end | TIMESTAMP | Session end time |
| duration_minutes | INTEGER | Session length |
| pages_viewed | INTEGER | Number of pages/screens viewed |
| actions_taken | INTEGER | Number of actions performed |
| device_type | VARCHAR(50) | Desktop, Mobile, Tablet |
| browser | VARCHAR(50) | Chrome, Firefox, Safari, Edge |
| os | VARCHAR(50) | Windows, macOS, Linux, iOS, Android |
| ip_address | VARCHAR(50) | IP address (anonymized) |
| location_city | VARCHAR(100) | City |
| location_country | VARCHAR(100) | Country |

**Sample Use Cases**:
- User behavior analysis
- Device/browser compatibility
- Geographic usage patterns
- Session duration optimization

---

## 39. API_USAGE
**Purpose**: API consumption metrics  
**Schema**: `usage_data`  
**Estimated Records**: 100,000+

| Column | Type | Description |
|--------|------|-------------|
| api_usage_id | VARCHAR(50) PK | Unique API usage identifier (API-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| usage_date | DATE | Date of API usage |
| endpoint | VARCHAR(200) | API endpoint called |
| method | VARCHAR(20) | GET, POST, PUT, DELETE |
| call_count | INTEGER | Number of API calls |
| success_count | INTEGER | Successful calls |
| error_count | INTEGER | Failed calls |
| avg_response_time_ms | NUMBER(10,2) | Average response time |
| data_transferred_gb | NUMBER(15,2) | Data volume transferred |
| authentication_type | VARCHAR(50) | API Key, OAuth, Token |

**Sample Use Cases**:
- API usage patterns
- Performance monitoring
- Rate limiting analysis
- Integration health

---

## 40. INTEGRATION_USAGE
**Purpose**: Third-party integration tracking  
**Schema**: `usage_data`  
**Estimated Records**: 4,000

| Column | Type | Description |
|--------|------|-------------|
| integration_id | VARCHAR(50) PK | Unique integration identifier (INTG-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| integration_name | VARCHAR(200) | Name of integration (Salesforce, Slack, etc.) |
| integration_type | VARCHAR(100) | CRM, Marketing, Communication, Data |
| status | VARCHAR(50) | Active, Inactive, Error |
| install_date | DATE | When integration was installed |
| last_sync_date | DATE | Last successful sync |
| sync_frequency | VARCHAR(50) | Real-time, Hourly, Daily |
| records_synced | INTEGER | Number of records synced |
| error_count | INTEGER | Number of sync errors |
| config_json | VARIANT/JSON | Configuration details |

**Sample Use Cases**:
- Integration adoption tracking
- Sync health monitoring
- Ecosystem mapping
- Partnership insights

---

## 41. LICENSE_USAGE
**Purpose**: License utilization tracking  
**Schema**: `usage_data`  
**Estimated Records**: 20,000

| Column | Type | Description |
|--------|------|-------------|
| license_usage_id | VARCHAR(50) PK | Unique license usage identifier (LIC-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| usage_date | DATE | Date of measurement |
| licenses_purchased | INTEGER | Total licenses purchased |
| licenses_active | INTEGER | Currently used licenses |
| licenses_available | INTEGER | Unused licenses |
| utilization_percent | NUMBER(5,2) | Percentage in use |
| peak_usage_date | DATE | Date of highest usage |
| peak_usage_count | INTEGER | Peak concurrent users |
| overage_count | INTEGER | Users over limit |
| overage_charges | NUMBER(15,2) | Additional charges for overage |

**Sample Use Cases**:
- License optimization
- True-up calculations
- Capacity planning
- Upsell opportunity identification

---

## 42. DATA_QUALITY_METRICS
**Purpose**: Data quality scoring and monitoring  
**Schema**: `usage_data`  
**Estimated Records**: 10,000

| Column | Type | Description |
|--------|------|-------------|
| quality_metric_id | VARCHAR(50) PK | Unique quality metric identifier |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| metric_date | DATE | Date of measurement |
| completeness_score | NUMBER(5,2) | Data completeness (0-100) |
| accuracy_score | NUMBER(5,2) | Data accuracy (0-100) |
| consistency_score | NUMBER(5,2) | Data consistency (0-100) |
| timeliness_score | NUMBER(5,2) | Data freshness (0-100) |
| duplicate_records | INTEGER | Number of duplicate records |
| null_values_percent | NUMBER(5,2) | Percentage of null values |
| validation_errors | INTEGER | Number of validation errors |
| overall_quality_score | NUMBER(5,2) | Composite quality score |

**Sample Use Cases**:
- Data quality monitoring
- Customer health assessment
- Implementation success tracking
- Data governance

---

## 43. PERFORMANCE_METRICS
**Purpose**: System performance monitoring  
**Schema**: `usage_data`  
**Estimated Records**: 10,000

| Column | Type | Description |
|--------|------|-------------|
| performance_id | VARCHAR(50) PK | Unique performance metric identifier |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| metric_date | DATE | Date of measurement |
| avg_page_load_time_ms | NUMBER(10,2) | Average page load time |
| avg_query_time_ms | NUMBER(10,2) | Average query execution time |
| uptime_percent | NUMBER(5,2) | System uptime percentage |
| downtime_minutes | INTEGER | Total downtime |
| peak_concurrent_users | INTEGER | Peak concurrent users |
| avg_concurrent_users | INTEGER | Average concurrent users |
| cpu_utilization_percent | NUMBER(5,2) | CPU usage |
| memory_utilization_percent | NUMBER(5,2) | Memory usage |
| storage_used_gb | NUMBER(15,2) | Storage consumed |

**Sample Use Cases**:
- Performance optimization
- Capacity planning
- SLA monitoring
- Infrastructure scaling

---

# Marketing Data

## 12. CAMPAIGNS
**Purpose**: Marketing campaign master data  
**Schema**: `marketing_data`  
**Estimated Records**: 75

| Column | Type | Description |
|--------|------|-------------|
| campaign_id | VARCHAR(50) PK | Unique campaign identifier (CAMP-XXXXXX) |
| campaign_name | VARCHAR(500) | Campaign name |
| campaign_type | VARCHAR(100) | Email, Webinar, Event, Social, Content, Partner |
| channel | VARCHAR(100) | Email, LinkedIn, Trade Show, Webinar, etc. |
| start_date | DATE | Campaign start date |
| end_date | DATE | Campaign end date |
| budget | NUMBER(15,2) | Campaign budget |
| target_audience | VARCHAR(100) | Enterprise, SMB, Mid-Market |
| region | VARCHAR(100) | Geographic region |
| product_focus | VARCHAR(50) FK | References PRODUCTS |
| status | VARCHAR(50) | Planned, Active, Completed, Cancelled |
| owner | VARCHAR(100) | Marketing manager |

**Sample Use Cases**:
- Campaign planning
- Budget allocation
- Performance tracking
- ROI analysis

---

## 13. EVENTS
**Purpose**: Marketing events and webinars  
**Schema**: `marketing_data`  
**Estimated Records**: 45

| Column | Type | Description |
|--------|------|-------------|
| event_id | VARCHAR(50) PK | Unique event identifier (EVT-XXXXXX) |
| event_name | VARCHAR(500) | Event name |
| event_type | VARCHAR(100) | Conference, Webinar, Workshop, Trade Show, User Group |
| event_date | DATE | Event date |
| location | VARCHAR(200) | City/venue or "Virtual" |
| region | VARCHAR(100) | Geographic region |
| expected_attendees | INTEGER | Projected attendance |
| actual_attendees | INTEGER | Actual attendance |
| budget | NUMBER(15,2) | Event budget |
| products_showcased | VARCHAR(1000) | Featured products |
| lead_goal | INTEGER | Target number of leads |
| campaign_id | VARCHAR(50) FK | References CAMPAIGNS |

**Sample Use Cases**:
- Event ROI analysis
- Attendance tracking
- Lead generation effectiveness
- Event planning optimization

---

## 14. LEADS
**Purpose**: Marketing qualified leads  
**Schema**: `marketing_data`  
**Estimated Records**: 7,000

| Column | Type | Description |
|--------|------|-------------|
| lead_id | VARCHAR(50) PK | Unique lead identifier (LEAD-XXXXXX) |
| company_name | VARCHAR(200) | Company name |
| contact_name | VARCHAR(200) | Lead contact name |
| email | VARCHAR(200) | Email address |
| phone | VARCHAR(50) | Phone number |
| title | VARCHAR(100) | Job title |
| industry | VARCHAR(100) | Industry sector |
| company_size | VARCHAR(50) | Small, Medium, Large, Enterprise |
| region | VARCHAR(100) | Geographic region |
| country | VARCHAR(100) | Country |
| lead_source | VARCHAR(100) | Source of lead |
| campaign_id | VARCHAR(50) FK | References CAMPAIGNS |
| event_id | VARCHAR(50) FK | References EVENTS |
| lead_score | INTEGER | Lead score (0-100) |
| lead_status | VARCHAR(50) | New, Contacted, Qualified, Unqualified, Converted |
| created_date | DATE | Lead creation date |
| last_activity_date | DATE | Last interaction date |
| assigned_to | VARCHAR(100) | Sales rep assigned |
| products_interested | VARCHAR(500) | Products of interest |
| converted_to_opp_id | VARCHAR(50) FK | References OPPORTUNITIES (if converted) |
| conversion_date | DATE | Date converted to opportunity |

**Sample Use Cases**:
- Lead qualification
- Conversion rate analysis
- Lead scoring optimization
- Source effectiveness

---

## 15. CAMPAIGN_ENGAGEMENT
**Purpose**: Individual engagement activities with campaigns  
**Schema**: `marketing_data`  
**Estimated Records**: 40,000

| Column | Type | Description |
|--------|------|-------------|
| engagement_id | VARCHAR(50) PK | Unique engagement identifier (ENG-XXXXXX) |
| campaign_id | VARCHAR(50) FK | References CAMPAIGNS |
| lead_id | VARCHAR(50) FK | References LEADS |
| contact_id | VARCHAR(200) | Email/contact identifier |
| engagement_type | VARCHAR(100) | Email Opened, Link Clicked, Form Filled, Downloaded, Attended |
| engagement_date | DATE | Date of engagement |
| asset_name | VARCHAR(500) | Asset engaged with |
| engagement_score | INTEGER | Points assigned for this action |

**Sample Use Cases**:
- Engagement tracking
- Lead nurturing effectiveness
- Content performance
- Campaign effectiveness

---

## 16. MARKETING_ATTRIBUTION
**Purpose**: Multi-touch attribution for opportunities  
**Schema**: `marketing_data`  
**Estimated Records**: 3,000

| Column | Type | Description |
|--------|------|-------------|
| attribution_id | VARCHAR(50) PK | Unique attribution identifier (ATTRIB-XXXXXX) |
| opportunity_id | VARCHAR(50) FK | References OPPORTUNITIES |
| deal_id | VARCHAR(50) FK | References CLOSED_DEALS (if closed) |
| campaign_id | VARCHAR(50) FK | References CAMPAIGNS |
| event_id | VARCHAR(50) FK | References EVENTS |
| touchpoint_order | INTEGER | 1st touch, 2nd touch, etc. |
| touchpoint_date | DATE | Date of touchpoint |
| attribution_weight | NUMBER(5,2) | % credit for this touchpoint |
| attribution_model | VARCHAR(100) | First Touch, Last Touch, Multi-Touch, Linear |

**Sample Use Cases**:
- Attribution modeling
- Campaign influence analysis
- Marketing ROI
- Customer journey mapping

---

## 17. WEBINAR_ATTENDANCE
**Purpose**: Webinar registration and attendance tracking  
**Schema**: `marketing_data`  
**Estimated Records**: 2,500

| Column | Type | Description |
|--------|------|-------------|
| attendance_id | VARCHAR(50) PK | Unique attendance identifier (ATTEND-XXXXXX) |
| event_id | VARCHAR(50) FK | References EVENTS (where event_type = Webinar) |
| lead_id | VARCHAR(50) FK | References LEADS |
| registration_date | DATE | When they registered |
| attended | VARCHAR(10) | Yes/No |
| attendance_duration_min | INTEGER | How long they stayed |
| poll_responses | VARCHAR(10) | Engaged with polls (Yes/No) |
| questions_asked | INTEGER | Number of questions asked |
| follow_up_requested | VARCHAR(10) | Requested follow-up (Yes/No) |

**Sample Use Cases**:
- Webinar effectiveness
- Engagement analysis
- Follow-up prioritization
- Attendance rate optimization

---

## 44. CONTENT_ASSETS
**Purpose**: Marketing content library and performance  
**Schema**: `marketing_data`  
**Estimated Records**: 150

| Column | Type | Description |
|--------|------|-------------|
| asset_id | VARCHAR(50) PK | Unique asset identifier (ASSET-XXXXXX) |
| asset_name | VARCHAR(500) | Asset name |
| asset_type | VARCHAR(100) | Whitepaper, eBook, Video, Webinar, Case Study, Blog |
| product_focus | VARCHAR(50) FK | References PRODUCTS |
| topic | VARCHAR(200) | Main topic |
| publish_date | DATE | Publication date |
| author | VARCHAR(100) | Content creator |
| downloads_count | INTEGER | Number of downloads |
| views_count | INTEGER | Number of views |
| avg_time_spent_min | NUMBER(10,2) | Average engagement time |
| conversion_rate | NUMBER(5,2) | % who became leads |
| gated | VARCHAR(10) | Yes/No (requires form) |
| file_url | VARCHAR(500) | Location of file |
| status | VARCHAR(50) | Draft, Published, Archived |

**Sample Use Cases**:
- Content performance analysis
- Asset ROI
- Topic effectiveness
- Content gap identification

---

## 45. EMAIL_CAMPAIGNS
**Purpose**: Email campaign performance metrics  
**Schema**: `marketing_data`  
**Estimated Records**: 200

| Column | Type | Description |
|--------|------|-------------|
| email_campaign_id | VARCHAR(50) PK | Unique email campaign identifier (EMAIL-XXXXXX) |
| campaign_id | VARCHAR(50) FK | References CAMPAIGNS |
| email_name | VARCHAR(500) | Email subject/name |
| send_date | DATE | When email was sent |
| recipient_count | INTEGER | Number of recipients |
| delivered_count | INTEGER | Successfully delivered |
| open_count | INTEGER | Number of opens |
| open_rate_percent | NUMBER(5,2) | Open rate |
| click_count | INTEGER | Number of clicks |
| click_rate_percent | NUMBER(5,2) | Click-through rate |
| bounce_count | INTEGER | Bounced emails |
| unsubscribe_count | INTEGER | Unsubscribes |
| spam_complaint_count | INTEGER | Spam reports |
| conversion_count | INTEGER | Leads/opportunities generated |

**Sample Use Cases**:
- Email performance optimization
- Subject line testing
- Engagement rate trends
- List health monitoring

---

## 46. SOCIAL_MEDIA_ENGAGEMENT
**Purpose**: Social media post performance  
**Schema**: `marketing_data`  
**Estimated Records**: 500

| Column | Type | Description |
|--------|------|-------------|
| social_engagement_id | VARCHAR(50) PK | Unique social engagement identifier (SOCIAL-XXXXXX) |
| campaign_id | VARCHAR(50) FK | References CAMPAIGNS (optional) |
| platform | VARCHAR(100) | LinkedIn, Twitter, Facebook, Instagram, YouTube |
| post_date | DATE | When posted |
| post_type | VARCHAR(50) | Organic, Paid, Sponsored |
| content_type | VARCHAR(50) | Post, Video, Article, Event, Poll |
| impressions | INTEGER | Number of views |
| reach | INTEGER | Unique users reached |
| engagement_count | INTEGER | Likes, comments, shares combined |
| click_count | INTEGER | Clicks to website |
| video_views | INTEGER | Video view count |
| lead_count | INTEGER | Leads generated |
| spend | NUMBER(15,2) | Ad spend (if paid) |
| post_url | VARCHAR(500) | Link to post |

**Sample Use Cases**:
- Social media ROI
- Platform effectiveness comparison
- Content type analysis
- Organic vs paid performance

---

## 47. WEBSITE_TRAFFIC
**Purpose**: Website analytics and page performance  
**Schema**: `marketing_data`  
**Estimated Records**: 10,000+

| Column | Type | Description |
|--------|------|-------------|
| traffic_id | VARCHAR(50) PK | Unique traffic identifier (TRAFFIC-XXXXXX) |
| traffic_date | DATE | Date of traffic |
| page_url | VARCHAR(500) | Page visited |
| page_views | INTEGER | Number of views |
| unique_visitors | INTEGER | Unique users |
| avg_time_on_page_sec | INTEGER | Average time spent |
| bounce_rate_percent | NUMBER(5,2) | Bounce rate |
| entry_page_count | INTEGER | Times used as entry |
| exit_page_count | INTEGER | Times used as exit |
| conversion_count | INTEGER | Forms submitted, demos requested |
| source | VARCHAR(100) | Organic, Paid, Direct, Referral, Social |
| campaign_id | VARCHAR(50) FK | References CAMPAIGNS (if trackable) |

**Sample Use Cases**:
- Website optimization
- Conversion path analysis
- Traffic source effectiveness
- Page performance monitoring

---

## 48. PARTNER_REFERRALS
**Purpose**: Partner channel referral tracking  
**Schema**: `marketing_data`  
**Estimated Records**: 300

| Column | Type | Description |
|--------|------|-------------|
| referral_id | VARCHAR(50) PK | Unique referral identifier (REF-XXXXXX) |
| partner_name | VARCHAR(200) | Partner company name |
| partner_type | VARCHAR(100) | Reseller, Technology, Consulting, Agency |
| referral_date | DATE | When referred |
| contact_name | VARCHAR(200) | Referral contact name |
| contact_email | VARCHAR(200) | Contact email |
| company_name | VARCHAR(200) | Referred company |
| industry | VARCHAR(100) | Industry |
| region | VARCHAR(100) | Geographic region |
| estimated_value | NUMBER(15,2) | Potential deal size |
| status | VARCHAR(50) | New, Qualified, Converted, Lost |
| lead_id | VARCHAR(50) FK | References LEADS (if converted) |
| opportunity_id | VARCHAR(50) FK | References OPPORTUNITIES (if converted) |
| commission_percent | NUMBER(5,2) | Partner commission rate |
| notes | TEXT | Additional details |

**Sample Use Cases**:
- Partner program effectiveness
- Referral conversion rates
- Partner performance ranking
- Commission tracking

---

## 49. MARKETING_BUDGET
**Purpose**: Marketing budget allocation and tracking  
**Schema**: `marketing_data`  
**Estimated Records**: 500

| Column | Type | Description |
|--------|------|-------------|
| budget_id | VARCHAR(50) PK | Unique budget identifier (BUDG-XXXXXX) |
| fiscal_quarter | VARCHAR(20) | Q1 2024, Q2 2024, etc. |
| fiscal_year | INTEGER | Fiscal year |
| channel | VARCHAR(100) | Digital, Events, Content, PR, Partner |
| campaign_type | VARCHAR(100) | Email, Social, Webinar, Conference, etc. |
| budgeted_amount | NUMBER(15,2) | Planned spend |
| actual_spend | NUMBER(15,2) | Actual spend |
| variance_amount | NUMBER(15,2) | Budget variance |
| leads_generated | INTEGER | Leads from this budget |
| opportunities_generated | INTEGER | Opportunities created |
| pipeline_value | NUMBER(15,2) | Total pipeline value |
| closed_won_value | NUMBER(15,2) | Revenue generated |
| roi_percent | NUMBER(5,2) | Return on investment |

**Sample Use Cases**:
- Budget vs actual analysis
- ROI tracking
- Channel effectiveness
- Investment optimization

---

## 50. LEAD_SCORING_RULES
**Purpose**: Lead scoring configuration and rules  
**Schema**: `marketing_data`  
**Estimated Records**: 30

| Column | Type | Description |
|--------|------|-------------|
| rule_id | VARCHAR(50) PK | Unique rule identifier (RULE-XXXXXX) |
| rule_name | VARCHAR(200) | Rule name |
| rule_category | VARCHAR(100) | Demographic, Behavioral, Firmographic |
| criteria | VARCHAR(500) | Condition (e.g., "Job Title = VP") |
| points | INTEGER | Points awarded |
| active | VARCHAR(10) | Yes/No |
| effective_date | DATE | When rule became active |
| created_by | VARCHAR(100) | Who created rule |
| last_modified | DATE | Last update date |

**Sample Use Cases**:
- Lead scoring optimization
- Rule effectiveness analysis
- Scoring model tuning
- Qualification criteria management

---

# Support & Service

## 18. SUPPORT_TICKETS
**Purpose**: Customer support ticket tracking  
**Schema**: `support_data`  
**Estimated Records**: 10,000

| Column | Type | Description |
|--------|------|-------------|
| ticket_id | VARCHAR(50) PK | Unique ticket identifier (TICK-XXXXXX) |
| ticket_number | VARCHAR(50) | Human-readable ticket number |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| contact_name | VARCHAR(200) | Person who opened ticket |
| contact_email | VARCHAR(200) | Their email |
| subject | VARCHAR(500) | Ticket subject |
| description | TEXT | Detailed description |
| priority | VARCHAR(50) | Low, Medium, High, Critical |
| severity | VARCHAR(50) | Minor, Major, Critical, Blocker |
| status | VARCHAR(50) | New, Open, In Progress, Pending Customer, Resolved, Closed |
| ticket_type | VARCHAR(100) | Bug, Feature Request, Question, Configuration, Performance, Security |
| category | VARCHAR(100) | Technical, Billing, Access, Training, Integration, Other |
| created_date | DATE | Ticket creation date |
| first_response_date | DATE | When first response was sent |
| resolved_date | DATE | When marked as resolved |
| closed_date | DATE | When ticket was closed |
| assigned_to | VARCHAR(100) | Support engineer |
| escalated | VARCHAR(10) | Yes/No |
| escalation_date | DATE | When escalated |
| sla_status | VARCHAR(50) | Met, Breached, At Risk |
| channel | VARCHAR(50) | Email, Phone, Chat, Portal, API |
| product_version | VARCHAR(50) | Version customer is using |
| environment | VARCHAR(50) | Production, Staging, Development |

**Sample Use Cases**:
- Ticket volume trends
- SLA compliance monitoring
- Support efficiency analysis
- Problem area identification

---

## 19. TICKET_COMMENTS
**Purpose**: Ticket conversation history  
**Schema**: `support_data`  
**Estimated Records**: 40,000

| Column | Type | Description |
|--------|------|-------------|
| comment_id | VARCHAR(50) PK | Unique comment identifier (COMM-XXXXXX) |
| ticket_id | VARCHAR(50) FK | References SUPPORT_TICKETS |
| comment_date | DATE | When comment was added |
| commenter_type | VARCHAR(50) | Customer, Support Agent, Engineer, Manager |
| commenter_name | VARCHAR(100) | Who made the comment |
| comment_text | TEXT | Comment content |
| internal_note | VARCHAR(10) | Yes/No (visible to customer?) |
| time_spent_min | INTEGER | Time logged on this update |

**Sample Use Cases**:
- Communication analysis
- Response time tracking
- Agent workload
- Customer interaction patterns

---

## 20. TICKET_RESOLUTION
**Purpose**: Ticket resolution details and root cause  
**Schema**: `support_data`  
**Estimated Records**: 8,000

| Column | Type | Description |
|--------|------|-------------|
| resolution_id | VARCHAR(50) PK | Unique resolution identifier (RES-XXXXXX) |
| ticket_id | VARCHAR(50) FK | References SUPPORT_TICKETS |
| resolution_date | DATE | When resolved |
| resolution_type | VARCHAR(100) | Fixed, Workaround, By Design, Duplicate, Cannot Reproduce, Configuration |
| resolution_notes | TEXT | How it was resolved |
| root_cause | VARCHAR(200) | Product Bug, User Error, Configuration, Infrastructure, Documentation |
| kb_article_id | VARCHAR(50) FK | References KNOWLEDGE_BASE (if created) |
| workaround_provided | VARCHAR(10) | Yes/No |
| permanent_fix | VARCHAR(10) | Yes/No (if workaround) |
| engineer_assigned | VARCHAR(100) | Who resolved it |
| effort_hours | NUMBER(10,1) | Total hours spent |

**Sample Use Cases**:
- Root cause analysis
- Resolution pattern identification
- Knowledge base needs
- Engineering effort tracking

---

## 21. CUSTOMER_SATISFACTION
**Purpose**: Post-ticket customer satisfaction surveys  
**Schema**: `support_data`  
**Estimated Records**: 5,000

| Column | Type | Description |
|--------|------|-------------|
| csat_id | VARCHAR(50) PK | Unique CSAT identifier (CSAT-XXXXXX) |
| ticket_id | VARCHAR(50) FK | References SUPPORT_TICKETS |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| survey_date | DATE | When survey was sent |
| response_date | DATE | When customer responded |
| satisfaction_score | INTEGER | 1-5 or 1-10 rating |
| would_recommend | VARCHAR(10) | Yes/No (NPS) |
| feedback_text | TEXT | Written feedback |
| response_time_rating | INTEGER | 1-5 rating |
| resolution_quality_rating | INTEGER | 1-5 rating |
| agent_rating | INTEGER | 1-5 rating |
| survey_type | VARCHAR(50) | Post-Ticket, Quarterly, Annual |

**Sample Use Cases**:
- Customer satisfaction trends
- Agent performance
- Support quality monitoring
- Churn risk identification

---

## 22. SLA_POLICIES
**Purpose**: Service level agreement definitions  
**Schema**: `support_data`  
**Estimated Records**: 25

| Column | Type | Description |
|--------|------|-------------|
| sla_policy_id | VARCHAR(50) PK | Unique SLA policy identifier (SLA-XXXXXX) |
| customer_tier | VARCHAR(50) | Bronze, Silver, Gold, Platinum |
| priority | VARCHAR(50) | Low, Medium, High, Critical |
| first_response_target_min | INTEGER | Target for first response (minutes) |
| resolution_target_hours | INTEGER | Target for resolution (hours) |
| business_hours_only | VARCHAR(10) | Yes/No |
| active | VARCHAR(10) | Yes/No |
| effective_date | DATE | When policy became active |

**Sample Use Cases**:
- SLA policy management
- Target setting
- Tier-based service levels
- Compliance tracking

---

## 23. SLA_BREACHES
**Purpose**: SLA breach tracking and analysis  
**Schema**: `support_data`  
**Estimated Records**: 800

| Column | Type | Description |
|--------|------|-------------|
| breach_id | VARCHAR(50) PK | Unique breach identifier (BREACH-XXXXXX) |
| ticket_id | VARCHAR(50) FK | References SUPPORT_TICKETS |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| breach_type | VARCHAR(50) | First Response, Resolution Time |
| target_time | INTEGER | What the target was |
| actual_time | INTEGER | What actually happened |
| breach_duration_min | INTEGER | How late (in minutes) |
| breach_date | DATE | When breach occurred |
| reason | VARCHAR(200) | Why it was breached |
| credited | VARCHAR(10) | Yes/No (if customer got credit) |

**Sample Use Cases**:
- SLA compliance monitoring
- Breach pattern analysis
- Resource allocation
- Process improvement

---

## 24. KNOWLEDGE_BASE
**Purpose**: Self-service knowledge base articles  
**Schema**: `support_data`  
**Estimated Records**: 250

| Column | Type | Description |
|--------|------|-------------|
| kb_id | VARCHAR(50) PK | Unique KB article identifier |
| article_title | VARCHAR(500) | Article title |
| article_category | VARCHAR(100) | Installation, Configuration, Troubleshooting, How-To, FAQ |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| product_version | VARCHAR(50) | Which version(s) it applies to |
| content | TEXT | Article content |
| created_date | DATE | When published |
| last_updated | DATE | Last modification |
| author | VARCHAR(100) | Who wrote it |
| views_count | INTEGER | Number of views |
| helpfulness_score | NUMBER(5,2) | Average rating (1-5) |
| related_tickets_count | INTEGER | How many tickets reference this |
| status | VARCHAR(50) | Draft, Published, Archived |
| tags | VARCHAR(500) | Searchable tags |

**Sample Use Cases**:
- Content effectiveness
- Self-service deflection
- Gap analysis
- Search optimization

---

## 25. TICKET_TAGS
**Purpose**: Flexible ticket categorization and labeling  
**Schema**: `support_data`  
**Estimated Records**: 20,000

| Column | Type | Description |
|--------|------|-------------|
| tag_id | VARCHAR(50) PK | Unique tag identifier |
| ticket_id | VARCHAR(50) FK | References SUPPORT_TICKETS |
| tag_name | VARCHAR(100) | Tag/label (e.g., "api-issue", "data-loss", "urgent") |
| tagged_by | VARCHAR(100) | Who added the tag |
| tagged_date | DATE | When added |

**Sample Use Cases**:
- Trend identification
- Problem categorization
- Reporting and analytics
- Ticket routing

---

## 26. ESCALATIONS
**Purpose**: Ticket escalation tracking  
**Schema**: `support_data`  
**Estimated Records**: 1,000

| Column | Type | Description |
|--------|------|-------------|
| escalation_id | VARCHAR(50) PK | Unique escalation identifier |
| ticket_id | VARCHAR(50) FK | References SUPPORT_TICKETS |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| escalation_level | VARCHAR(100) | L1 → L2, L2 → L3, L3 → Engineering, Manager |
| escalated_from | VARCHAR(100) | Previous owner |
| escalated_to | VARCHAR(100) | New owner |
| escalation_date | DATE | When escalated |
| escalation_reason | VARCHAR(200) | Complex Issue, SLA Risk, Customer Request, Severity |
| resolved_at_level | VARCHAR(100) | Which level resolved it |
| resolution_date | DATE | When resolved after escalation |

**Sample Use Cases**:
- Escalation patterns
- First-call resolution tracking
- Skill gap identification
- Process optimization

---

## 27. RECURRING_ISSUES
**Purpose**: Known recurring problems and patterns  
**Schema**: `support_data`  
**Estimated Records**: 80

| Column | Type | Description |
|--------|------|-------------|
| recurring_issue_id | VARCHAR(50) PK | Unique recurring issue identifier |
| issue_pattern | VARCHAR(500) | Description of recurring issue |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| product_version | VARCHAR(50) | Affected version(s) |
| first_occurrence | DATE | When first seen |
| last_occurrence | DATE | Most recent occurrence |
| occurrence_count | INTEGER | How many times seen |
| affected_customers_count | INTEGER | How many customers affected |
| priority | VARCHAR(50) | Low, Medium, High, Critical |
| status | VARCHAR(50) | Open, Under Investigation, Fix in Progress, Resolved |
| assigned_team | VARCHAR(100) | Engineering, Product, DevOps |
| related_tickets | VARCHAR(1000) | List of ticket_ids |
| root_cause | TEXT | If identified |
| fix_version | VARCHAR(50) | Which version will have fix |

**Sample Use Cases**:
- Product quality monitoring
- Release planning
- Customer communication
- Engineering prioritization

---

## 28. SUPPORT_AGENT_METRICS
**Purpose**: Support agent performance metrics  
**Schema**: `support_data`  
**Estimated Records**: 800

| Column | Type | Description |
|--------|------|-------------|
| metric_id | VARCHAR(50) PK | Unique metric identifier |
| agent_name | VARCHAR(100) | Support engineer name |
| metric_date | DATE | Date or week/month |
| tickets_opened | INTEGER | New tickets assigned |
| tickets_closed | INTEGER | Tickets resolved |
| avg_resolution_time_hrs | NUMBER(10,2) | Average time to resolve |
| first_response_time_avg | NUMBER(10,2) | Average first response time |
| csat_score_avg | NUMBER(5,2) | Average customer satisfaction |
| sla_breach_count | INTEGER | Number of SLA breaches |
| escalation_count | INTEGER | Tickets escalated up |
| reopened_tickets_count | INTEGER | Tickets reopened after closure |
| utilization_percent | NUMBER(5,2) | % of time logged |

**Sample Use Cases**:
- Agent performance reviews
- Workload balancing
- Training needs identification
- Capacity planning

---

## 51. SUPPORT_CHANNELS
**Purpose**: Support channel configuration and metrics  
**Schema**: `support_data`  
**Estimated Records**: 10

| Column | Type | Description |
|--------|------|-------------|
| channel_id | VARCHAR(50) PK | Unique channel identifier |
| channel_name | VARCHAR(100) | Email, Phone, Chat, Portal, Social |
| active | VARCHAR(10) | Yes/No |
| hours_of_operation | VARCHAR(200) | Business hours description |
| avg_response_time_min | NUMBER(10,2) | Average response time |
| satisfaction_score | NUMBER(5,2) | Average CSAT for channel |
| volume_percent | NUMBER(5,2) | % of total tickets |
| cost_per_ticket | NUMBER(10,2) | Average handling cost |

**Sample Use Cases**:
- Channel optimization
- Resource allocation
- Cost analysis
- Customer preference trends

---

## 52. TICKET_WORKLOG
**Purpose**: Detailed time tracking on tickets  
**Schema**: `support_data`  
**Estimated Records**: 30,000

| Column | Type | Description |
|--------|------|-------------|
| worklog_id | VARCHAR(50) PK | Unique worklog identifier |
| ticket_id | VARCHAR(50) FK | References SUPPORT_TICKETS |
| agent_name | VARCHAR(100) | Who worked on it |
| work_date | DATE | Date of work |
| time_spent_minutes | INTEGER | Time logged |
| work_description | TEXT | What was done |
| billable | VARCHAR(10) | Yes/No |
| internal_only | VARCHAR(10) | Yes/No (visible to customer?) |

**Sample Use Cases**:
- Time tracking
- Billable hours
- Effort estimation
- Resource planning

---

# Operational Data

## 53. CUSTOMER_HEALTH_SCORE
**Purpose**: Comprehensive customer health scoring  
**Schema**: `operational_data`  
**Estimated Records**: 30,000

| Column | Type | Description |
|--------|------|-------------|
| health_score_id | VARCHAR(50) PK | Unique health score identifier (HLTH-XXXXXX) |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| score_date | DATE | Date calculated |
| overall_score | NUMBER(5,2) | 0-100 composite score |
| product_usage_score | NUMBER(5,2) | Usage metric (0-100) |
| support_health_score | NUMBER(5,2) | Support metric (0-100) |
| payment_health_score | NUMBER(5,2) | Payment history (0-100) |
| engagement_score | NUMBER(5,2) | Engagement level (0-100) |
| sentiment_score | NUMBER(5,2) | Overall sentiment (0-100) |
| risk_level | VARCHAR(50) | Low, Medium, High, Critical |
| churn_probability | NUMBER(5,2) | % likelihood to churn |
| expansion_probability | NUMBER(5,2) | % likelihood to expand |
| health_trend | VARCHAR(50) | Improving, Stable, Declining |

**Sample Use Cases**:
- Churn prediction
- Expansion opportunity identification
- CSM prioritization
- Customer segmentation

---

## 54. ONBOARDING_PROGRESS
**Purpose**: Customer onboarding milestone tracking  
**Schema**: `operational_data`  
**Estimated Records**: 1,500

| Column | Type | Description |
|--------|------|-------------|
| onboarding_id | VARCHAR(50) PK | Unique onboarding identifier |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| start_date | DATE | Onboarding start |
| target_completion_date | DATE | Expected completion |
| actual_completion_date | DATE | Actual completion |
| status | VARCHAR(50) | Not Started, In Progress, Completed, Stalled |
| progress_percent | NUMBER(5,2) | % complete |
| milestone_count | INTEGER | Total milestones |
| milestones_completed | INTEGER | Completed milestones |
| csm_assigned | VARCHAR(100) | Customer Success Manager |
| health_status | VARCHAR(50) | Green, Yellow, Red |
| days_to_first_value | INTEGER | Time to initial value realization |
| adoption_rate | NUMBER(5,2) | % of features being used |

**Sample Use Cases**:
- Onboarding effectiveness
- Time-to-value optimization
- CSM workload management
- Early adoption tracking

---

## 55. PRODUCT_RELEASES
**Purpose**: Product version release tracking  
**Schema**: `operational_data`  
**Estimated Records**: 100

| Column | Type | Description |
|--------|------|-------------|
| release_id | VARCHAR(50) PK | Unique release identifier |
| product_id | VARCHAR(50) FK | References PRODUCTS |
| version_number | VARCHAR(50) | Release version |
| release_date | DATE | When released |
| release_type | VARCHAR(50) | Major, Minor, Patch, Hotfix |
| features_added | INTEGER | Number of new features |
| bugs_fixed | INTEGER | Number of bugs fixed |
| breaking_changes | VARCHAR(10) | Yes/No |
| eol_date | DATE | End of life date |
| support_end_date | DATE | End of support date |
| adoption_rate_percent | NUMBER(5,2) | % customers on this version |
| release_notes_url | VARCHAR(500) | Link to release notes |

**Sample Use Cases**:
- Version adoption tracking
- Release planning
- Support lifecycle management
- Migration planning

---

## 56. CUSTOMER_SUCCESS_ACTIVITIES
**Purpose**: CSM engagement and activity tracking  
**Schema**: `operational_data`  
**Estimated Records**: 5,000

| Column | Type | Description |
|--------|------|-------------|
| activity_id | VARCHAR(50) PK | Unique activity identifier |
| customer_id | VARCHAR(50) FK | References CUSTOMERS |
| activity_date | DATE | Date of activity |
| activity_type | VARCHAR(100) | QBR, Health Check, Training, Check-in, Escalation |
| csm_name | VARCHAR(100) | Customer Success Manager |
| outcome | TEXT | Outcome/summary |
| sentiment | VARCHAR(50) | Positive, Neutral, Negative |
| action_items | TEXT | Follow-up actions |
| next_activity_date | DATE | Next scheduled activity |
| attendees | VARCHAR(500) | Who attended |
| duration_minutes | INTEGER | Length of activity |

**Sample Use Cases**:
- CSM activity tracking
- Customer engagement patterns
- Outcome analysis
- QBR scheduling

---

## 57. USER_ACCOUNTS
**Purpose