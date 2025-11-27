# Sales Analytics Demo - Quick Reference Card

## 🚀 Fast Setup (5 Minutes)

### 1. Generate Data
```bash
python sales_data_generator_part1.py
python sales_data_generator_part2.py
python sales_data_generator_part3.py
```

### 2. Upload to S3
```bash
aws s3 sync sales_analytics_data/ s3://your-bucket/sales-demo-data/
```

### 3A. Snowflake - Quick Load
```sql
-- Run DDL script, then:
CREATE STAGE sales_analytics_demo.public.s3_stage
  URL = 's3://your-bucket/sales-demo-data/'
  CREDENTIALS = (AWS_KEY_ID='xxx' AWS_SECRET_KEY='xxx');

COPY INTO sales_data.customers FROM @s3_stage/sales_data/customers.csv;
-- Repeat for 61 more tables
```

### 3B. Databricks - Quick Load
```python
# In notebook:
S3_BUCKET = "your-bucket"
# Run all cells in databricks_loader notebook
```

---

## 📊 Top 10 Demo Queries

### 1. Quarterly Sales Trend
```sql
SELECT quarter, SUM(deal_value) as revenue, COUNT(*) as deals
FROM sales_data.closed_deals
WHERE status = 'Won'
GROUP BY quarter ORDER BY quarter;
```

### 2. Win Rate by Product
```sql
SELECT p.product_name,
  SUM(CASE WHEN cd.status='Won' THEN 1 ELSE 0 END)*100.0/COUNT(*) as win_rate
FROM closed_deals cd
JOIN products p ON cd.product_id = p.product_id
GROUP BY p.product_name;
```

### 3. At-Risk Customers
```sql
SELECT c.customer_name, AVG(h.churn_probability) as churn_risk
FROM customers c
JOIN customer_health_score h ON c.customer_id = h.customer_id
WHERE h.risk_level IN ('High', 'Critical')
GROUP BY c.customer_name
ORDER BY churn_risk DESC LIMIT 20;
```

### 4. Upsell Opportunities
```sql
SELECT c.customer_name, COUNT(DISTINCT cp.product_id) as products_owned,
  SUM(cp.contract_value) as total_value
FROM customers c
JOIN customer_products cp ON c.customer_id = cp.customer_id
GROUP BY c.customer_name
HAVING products_owned < 3
ORDER BY total_value DESC;
```

### 5. Campaign ROI
```sql
SELECT c.campaign_name, c.budget,
  COUNT(DISTINCT l.lead_id) as leads,
  SUM(cd.deal_value) as revenue,
  SUM(cd.deal_value)/NULLIF(c.budget,0) as roi
FROM campaigns c
LEFT JOIN leads l ON c.campaign_id = l.campaign_id
LEFT JOIN marketing_attribution ma ON c.campaign_id = ma.campaign_id
LEFT JOIN closed_deals cd ON ma.opportunity_id = cd.opportunity_id
GROUP BY c.campaign_name, c.budget
ORDER BY roi DESC;
```

### 6. Support SLA Compliance
```sql
SELECT customer_tier,
  COUNT(*) as tickets,
  SUM(CASE WHEN sla_status='Met' THEN 1 ELSE 0 END)*100.0/COUNT(*) as compliance
FROM support_tickets st
JOIN customers c ON st.customer_id = c.customer_id
GROUP BY customer_tier;
```

### 7. Product Usage Trends
```sql
SELECT product_id, usage_date,
  AVG(license_utilization) as utilization,
  AVG(active_users) as users
FROM product_usage_summary
WHERE usage_date >= CURRENT_DATE - 90
GROUP BY product_id, usage_date
ORDER BY product_id, usage_date;
```

### 8. Top Loss Reasons
```sql
SELECT win_loss_reason, COUNT(*) as count,
  SUM(amount) as lost_revenue
FROM closed_deals cd
JOIN opportunities o ON cd.opportunity_id = o.opportunity_id
WHERE cd.status = 'Lost'
GROUP BY win_loss_reason
ORDER BY count DESC;
```

### 9. Customer 360 View
```sql
SELECT c.customer_name, c.industry, c.customer_tier,
  COUNT(DISTINCT cp.product_id) as products,
  COUNT(DISTINCT st.ticket_id) as tickets,
  AVG(csat.satisfaction_score) as avg_csat
FROM customers c
LEFT JOIN customer_products cp ON c.customer_id = cp.customer_id
LEFT JOIN support_tickets st ON c.customer_id = st.customer_id
LEFT JOIN customer_satisfaction csat ON c.customer_id = csat.customer_id
GROUP BY c.customer_name, c.industry, c.customer_tier;
```

### 10. Sales Performance vs Target
```sql
SELECT cd.quarter, cd.region,
  SUM(cd.deal_value) as actual,
  AVG(st.target_amount) as target,
  SUM(cd.deal_value)/AVG(st.target_amount)*100 as attainment
FROM closed_deals cd
JOIN sales_targets st ON cd.quarter = st.quarter AND cd.region = st.region
WHERE cd.status = 'Won'
GROUP BY cd.quarter, cd.region;
```

---

## 🔍 Data Validation

```sql
-- Check record counts
SELECT 'customers' as table_name, COUNT(*) FROM customers
UNION ALL SELECT 'opportunities', COUNT(*) FROM opportunities
UNION ALL SELECT 'closed_deals', COUNT(*) FROM closed_deals
UNION ALL SELECT 'support_tickets', COUNT(*) FROM support_tickets
UNION ALL SELECT 'leads', COUNT(*) FROM leads;

-- Expected counts:
-- customers: ~1,000
-- opportunities: ~4,000
-- closed_deals: ~2,500
-- support_tickets: ~10,000
-- leads: ~7,000
```

---

## 🎯 Demo Scenarios

### Scenario 1: Sales Review
Show quarterly trends, win rates, pipeline, top deals

### Scenario 2: Customer Success
Health scores, at-risk customers, usage patterns, renewals

### Scenario 3: Marketing Analytics
Campaign performance, ROI, lead conversion, attribution

### Scenario 4: Product Analytics
Feature adoption, API usage, version distribution, quality metrics

### Scenario 5: Support Operations
Ticket volume, SLA compliance, CSAT trends, escalations

---

## 📁 File Structure Reference

```
sales_analytics_data/
├── sales_data/          (17 CSV files)
│   ├── customers.csv
│   ├── products.csv
│   ├── opportunities.csv
│   └── ...
├── usage_data/          (11 CSV files)
│   ├── product_usage_summary.csv
│   ├── api_usage.csv
│   └── ...
├── marketing_data/      (13 CSV files)
│   ├── campaigns.csv
│   ├── leads.csv
│   └── ...
├── support_data/        (12 CSV files)
│   ├── support_tickets.csv
│   ├── customer_satisfaction.csv
│   └── ...
└── operational_data/    (9 CSV files)
    ├── customer_health_score.csv
    ├── invoices.csv
    └── ...
```

---

## ⚡ Performance Tips

### Snowflake
```sql
-- Add clustering
ALTER TABLE closed_deals CLUSTER BY (quarter, region);

-- Materialized views for common queries
CREATE MATERIALIZED VIEW mv_sales_summary AS
SELECT quarter, region, SUM(deal_value) as revenue
FROM closed_deals GROUP BY quarter, region;
```

### Databricks
```python
# Optimize tables
spark.sql("OPTIMIZE sales_data.closed_deals ZORDER BY (quarter, region)")

# Cache frequently used tables
spark.sql("CACHE TABLE sales_data.customers")
```

---

## 🛠️ Common Modifications

### Add More Customers
```python
# In part1.py, change:
NUM_CUSTOMERS = 2000  # was 1000
```

### Change Date Range
```python
# In all parts, change:
START_DATE = datetime(2022, 1, 1)  # was 2023
END_DATE = datetime(2025, 12, 31)  # was 2025-09-30
```

### Add Custom Attributes
```python
# In part1.py, add to INDUSTRIES list:
INDUSTRIES = ['Technology', 'Healthcare', ..., 'Your Industry']
```

---

## 🎨 Visualization Ideas

1. **Sales Dashboard**: Revenue by quarter, win rate trends, pipeline funnel
2. **Customer Health**: Risk matrix, churn prediction, usage heatmap
3. **Marketing Dashboard**: Campaign ROI, lead conversion funnel, channel mix
4. **Support Dashboard**: Ticket volume, SLA trends, CSAT scores
5. **Executive Summary**: KPIs, targets vs actuals, forecasts

---

## 📞 Quick Troubleshooting

| Issue | Solution |
|-------|----------|
| Missing files | Check all 3 Python scripts ran successfully |
| S3 upload fails | Verify AWS credentials and bucket permissions |
| Schema not found | Ensure database/catalog was created first |
| Date parsing errors | Set consistent date format in all CSVs |
| Performance slow | Add clustering/partitioning, use materialized views |

---

**Total Setup Time: ~15 minutes**
**Total Record Count: ~1.5 million**
**Total Tables: 62**