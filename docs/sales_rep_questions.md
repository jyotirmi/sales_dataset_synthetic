# Sales Rep Analytical Questions

## 🎯 Territory & Pipeline Management

### 1. **"How healthy is my pipeline this quarter?"**
**Analysis**: Pipeline coverage, stage distribution, close date distribution
```sql
SELECT 
    stage,
    COUNT(*) as opp_count,
    SUM(amount) as pipeline_value,
    AVG(probability) as avg_probability,
    SUM(amount * probability / 100) as weighted_pipeline
FROM opportunities
WHERE sales_rep = 'MY_NAME'
    AND close_date BETWEEN '2024-10-01' AND '2024-12-31'
GROUP BY stage
ORDER BY 
    CASE stage
        WHEN 'Prospecting' THEN 1
        WHEN 'Qualification' THEN 2
        WHEN 'Proposal' THEN 3
        WHEN 'Negotiation' THEN 4
        WHEN 'Closed Won' THEN 5
        WHEN 'Closed Lost' THEN 6
    END;
```

### 2. **"Am I on track to hit my quota this quarter?"**
**Analysis**: Actual vs target, gap analysis, run rate
```sql
SELECT 
    st.quarter,
    st.target_amount as quota,
    COALESCE(SUM(cd.deal_value), 0) as closed_won,
    st.target_amount - COALESCE(SUM(cd.deal_value), 0) as gap,
    ROUND(COALESCE(SUM(cd.deal_value), 0) / st.target_amount * 100, 2) as attainment_pct,
    DATEDIFF(day, CURRENT_DATE, DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months') as days_left
FROM sales_targets st
LEFT JOIN closed_deals cd ON st.quarter = cd.quarter 
    AND cd.sales_rep = 'MY_NAME' 
    AND cd.status = 'Won'
WHERE st.quarter = 'Q4 2024'
    AND st.sales_rep = 'MY_NAME'
GROUP BY st.quarter, st.target_amount;
```

### 3. **"Which deals are at risk of slipping this quarter?"**
**Analysis**: Deals close to quarter end, low probability, stuck in stage
```sql
SELECT 
    o.opportunity_name,
    c.customer_name,
    o.amount,
    o.probability,
    o.stage,
    o.close_date,
    DATEDIFF(day, o.created_date, CURRENT_DATE) as days_in_pipeline,
    DATEDIFF(day, CURRENT_DATE, o.close_date) as days_to_close
FROM opportunities o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.sales_rep = 'MY_NAME'
    AND o.stage NOT IN ('Closed Won', 'Closed Lost')
    AND o.close_date <= '2024-12-31'
    AND (
        o.probability < 50
        OR DATEDIFF(day, CURRENT_DATE, o.close_date) <= 14
        OR DATEDIFF(day, o.created_date, CURRENT_DATE) > 90
    )
ORDER BY o.amount DESC;
```

---

## 🎯 Account Intelligence

### 4. **"Which of my customers are ripe for upsell?"**
**Analysis**: High health score, low product count, high usage
```sql
SELECT 
    c.customer_name,
    c.customer_tier,
    COUNT(DISTINCT cp.product_id) as products_owned,
    SUM(cp.contract_value) as current_arr,
    AVG(h.overall_score) as health_score,
    AVG(h.expansion_probability) as expansion_probability,
    AVG(pu.license_utilization) as avg_utilization
FROM customers c
JOIN customer_products cp ON c.customer_id = cp.customer_id
LEFT JOIN customer_health_score h ON c.customer_id = h.customer_id
LEFT JOIN product_usage_summary pu ON c.customer_id = pu.customer_id
WHERE c.account_owner = 'MY_NAME'
    AND h.score_date >= CURRENT_DATE - 30
    AND pu.usage_date >= CURRENT_DATE - 30
GROUP BY c.customer_name, c.customer_tier
HAVING 
    AVG(h.overall_score) > 70
    AND COUNT(DISTINCT cp.product_id) < 3
    AND AVG(pu.license_utilization) > 75
ORDER BY expansion_probability DESC, current_arr DESC
LIMIT 20;
```

### 5. **"Show me accounts similar to my best customer for prospecting"**
**Analysis**: Find lookalike accounts based on industry, size, region
```sql
-- First, identify your best customer
WITH best_customer AS (
    SELECT c.industry, c.company_size, c.region
    FROM customers c
    JOIN closed_deals cd ON c.customer_id = cd.customer_id
    WHERE c.account_owner = 'MY_NAME'
        AND cd.status = 'Won'
    GROUP BY c.customer_id, c.industry, c.company_size, c.region
    ORDER BY SUM(cd.deal_value) DESC
    LIMIT 1
),
-- Find similar prospects
similar_accounts AS (
    SELECT 
        c.customer_name,
        c.industry,
        c.company_size,
        c.region,
        c.annual_revenue,
        COUNT(DISTINCT cp.product_id) as products_owned,
        SUM(cp.contract_value) as current_value
    FROM customers c
    CROSS JOIN best_customer bc
    LEFT JOIN customer_products cp ON c.customer_id = cp.customer_id
    WHERE c.industry = bc.industry
        AND c.company_size = bc.company_size
        AND c.region = bc.region
        AND c.account_owner = 'MY_NAME'
    GROUP BY c.customer_name, c.industry, c.company_size, c.region, c.annual_revenue
    HAVING COUNT(DISTINCT cp.product_id) < 2  -- Room to grow
)
SELECT * FROM similar_accounts
ORDER BY annual_revenue DESC
LIMIT 25;
```

### 6. **"Which of my accounts are at risk of churning?"**
**Analysis**: Low health score, high support tickets, low usage
```sql
SELECT 
    c.customer_name,
    c.customer_tier,
    AVG(h.overall_score) as health_score,
    AVG(h.churn_probability) as churn_risk,
    COUNT(DISTINCT st.ticket_id) as open_tickets,
    AVG(pu.license_utilization) as avg_utilization,
    MIN(cp.renewal_date) as next_renewal,
    SUM(cp.contract_value) as at_risk_arr
FROM customers c
JOIN customer_products cp ON c.customer_id = cp.customer_id
LEFT JOIN customer_health_score h ON c.customer_id = h.customer_id
LEFT JOIN support_tickets st ON c.customer_id = st.customer_id
LEFT JOIN product_usage_summary pu ON c.customer_id = pu.customer_id
WHERE c.account_owner = 'MY_NAME'
    AND h.score_date >= CURRENT_DATE - 30
    AND (st.status IN ('Open', 'In Progress') OR st.ticket_id IS NULL)
    AND pu.usage_date >= CURRENT_DATE - 30
GROUP BY c.customer_name, c.customer_tier
HAVING 
    AVG(h.overall_score) < 60
    OR AVG(h.churn_probability) > 40
    OR AVG(pu.license_utilization) < 40
ORDER BY churn_risk DESC, at_risk_arr DESC;
```

### 7. **"Who are the key decision makers at my top accounts?"**
**Analysis**: Contact mapping with roles and engagement
```sql
SELECT 
    c.customer_name,
    SUM(cp.contract_value) as total_arr,
    cc.first_name || ' ' || cc.last_name as contact_name,
    cc.title,
    cc.role_type,
    cc.email,
    cc.phone,
    cc.last_contact_date,
    DATEDIFF(day, cc.last_contact_date, CURRENT_DATE) as days_since_contact,
    COUNT(cn.note_id) as recent_interactions
FROM customers c
JOIN customer_products cp ON c.customer_id = cp.customer_id
JOIN customer_contacts cc ON c.customer_id = cc.customer_id
LEFT JOIN customer_notes cn ON cc.contact_id = cn.contact_id 
    AND cn.note_date >= CURRENT_DATE - 90
WHERE c.account_owner = 'MY_NAME'
    AND cc.status = 'Active'
GROUP BY c.customer_name, cp.contract_value, cc.first_name, cc.last_name, 
    cc.title, cc.role_type, cc.email, cc.phone, cc.last_contact_date
ORDER BY total_arr DESC, 
    CASE cc.role_type 
        WHEN 'Decision Maker' THEN 1
        WHEN 'Champion' THEN 2
        WHEN 'Influencer' THEN 3
        WHEN 'User' THEN 4
        ELSE 5
    END;
```

---

## 🎯 Win/Loss Intelligence

### 8. **"What's my win rate and how does it compare to the team?"**
**Analysis**: Win rate by stage, product, deal size
```sql
WITH my_stats AS (
    SELECT 
        COUNT(CASE WHEN status = 'Won' THEN 1 END) as won,
        COUNT(CASE WHEN status = 'Lost' THEN 1 END) as lost,
        ROUND(COUNT(CASE WHEN status = 'Won' THEN 1 END) * 100.0 / 
            NULLIF(COUNT(*), 0), 2) as win_rate,
        AVG(CASE WHEN status = 'Won' THEN deal_value END) as avg_deal_size,
        AVG(sales_cycle_days) as avg_sales_cycle
    FROM closed_deals
    WHERE sales_rep = 'MY_NAME'
        AND fiscal_year = 2024
),
team_stats AS (
    SELECT 
        ROUND(COUNT(CASE WHEN status = 'Won' THEN 1 END) * 100.0 / 
            NULLIF(COUNT(*), 0), 2) as team_win_rate,
        AVG(CASE WHEN status = 'Won' THEN deal_value END) as team_avg_deal_size,
        AVG(sales_cycle_days) as team_avg_sales_cycle
    FROM closed_deals
    WHERE fiscal_year = 2024
)
SELECT 
    m.won,
    m.lost,
    m.win_rate as my_win_rate,
    t.team_win_rate,
    m.win_rate - t.team_win_rate as vs_team,
    m.avg_deal_size as my_avg_deal,
    t.team_avg_deal_size as team_avg_deal,
    m.avg_sales_cycle as my_cycle_days,
    t.team_avg_sales_cycle as team_cycle_days
FROM my_stats m, team_stats t;
```

### 9. **"Why am I losing deals? What are the top reasons?"**
**Analysis**: Loss reason analysis with trends
```sql
SELECT 
    win_loss_reason,
    COUNT(*) as lost_count,
    SUM(o.amount) as lost_revenue,
    ROUND(AVG(cd.sales_cycle_days), 1) as avg_cycle_days,
    COUNT(CASE WHEN cd.quarter = 'Q4 2024' THEN 1 END) as this_quarter,
    COUNT(CASE WHEN cd.quarter = 'Q3 2024' THEN 1 END) as last_quarter
FROM closed_deals cd
JOIN opportunities o ON cd.opportunity_id = o.opportunity_id
WHERE cd.sales_rep = 'MY_NAME'
    AND cd.status = 'Lost'
    AND cd.fiscal_year = 2024
GROUP BY win_loss_reason
ORDER BY lost_count DESC;
```

### 10. **"Which competitors am I losing to most often?"**
**Analysis**: Competitive loss analysis
```sql
SELECT 
    ci.competitor_name,
    ci.competitor_product,
    COUNT(*) as times_encountered,
    COUNT(CASE WHEN cd.status = 'Lost' THEN 1 END) as losses,
    COUNT(CASE WHEN cd.status = 'Won' THEN 1 END) as wins,
    ROUND(COUNT(CASE WHEN cd.status = 'Won' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2) as win_rate_vs_competitor,
    STRING_AGG(DISTINCT ci.competitor_weakness, '; ') as their_weaknesses,
    STRING_AGG(DISTINCT ci.competitor_strength, '; ') as their_strengths
FROM competitive_intelligence ci
JOIN opportunities o ON ci.opportunity_id = o.opportunity_id
LEFT JOIN closed_deals cd ON o.opportunity_id = cd.opportunity_id
WHERE o.sales_rep = 'MY_NAME'
    AND ci.competitor_name IS NOT NULL
GROUP BY ci.competitor_name, ci.competitor_product
HAVING COUNT(*) >= 3
ORDER BY losses DESC, times_encountered DESC;
```

---

## 🎯 Activity & Productivity

### 11. **"How many activities do I need to log this week to stay on track?"**
**Analysis**: Activity metrics and required pace
```sql
WITH current_metrics AS (
    SELECT 
        COUNT(*) as activities_this_week,
        COUNT(DISTINCT customer_id) as accounts_touched
    FROM customer_notes
    WHERE created_by = 'MY_NAME'
        AND note_date >= DATE_TRUNC('week', CURRENT_DATE)
),
pipeline_needs AS (
    SELECT 
        COUNT(*) as opps_in_pipeline,
        SUM(amount) as pipeline_value,
        COUNT(CASE WHEN close_date <= CURRENT_DATE + 30 THEN 1 END) as closing_soon
    FROM opportunities
    WHERE sales_rep = 'MY_NAME'
        AND stage NOT IN ('Closed Won', 'Closed Lost')
),
quota_gap AS (
    SELECT 
        st.target_amount - COALESCE(SUM(cd.deal_value), 0) as remaining_quota,
        DATEDIFF(day, CURRENT_DATE, '2024-12-31') as days_left
    FROM sales_targets st
    LEFT JOIN closed_deals cd ON st.quarter = cd.quarter 
        AND cd.sales_rep = 'MY_NAME' 
        AND cd.status = 'Won'
    WHERE st.quarter = 'Q4 2024'
        AND st.sales_rep = 'MY_NAME'
    GROUP BY st.target_amount
)
SELECT 
    cm.activities_this_week,
    cm.accounts_touched,
    pn.opps_in_pipeline,
    pn.closing_soon as opps_closing_within_30_days,
    qg.remaining_quota,
    qg.days_left,
    -- Recommended weekly activities
    ROUND((pn.opps_in_pipeline * 2) / 7.0, 0) as recommended_activities_per_week,
    ROUND((pn.closing_soon * 3) / 7.0, 0) as urgent_follow_ups_needed
FROM current_metrics cm, pipeline_needs pn, quota_gap qg;
```

### 12. **"What's my conversion rate by lead source?"**
**Analysis**: Lead source effectiveness for targeting
```sql
SELECT 
    o.lead_source,
    COUNT(DISTINCT o.opportunity_id) as total_opps,
    COUNT(DISTINCT CASE WHEN cd.status = 'Won' THEN cd.deal_id END) as won_deals,
    ROUND(COUNT(DISTINCT CASE WHEN cd.status = 'Won' THEN cd.deal_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT o.opportunity_id), 0), 2) as conversion_rate,
    SUM(CASE WHEN cd.status = 'Won' THEN cd.deal_value ELSE 0 END) as revenue_generated,
    AVG(CASE WHEN cd.status = 'Won' THEN cd.deal_value END) as avg_deal_size
FROM opportunities o
LEFT JOIN closed_deals cd ON o.opportunity_id = cd.opportunity_id
WHERE o.sales_rep = 'MY_NAME'
    AND o.created_date >= CURRENT_DATE - 365
GROUP BY o.lead_source
ORDER BY conversion_rate DESC, revenue_generated DESC;
```

### 13. **"Which marketing campaigns are driving my best opportunities?"**
**Analysis**: Campaign attribution for my deals
```sql
SELECT 
    c.campaign_name,
    c.campaign_type,
    COUNT(DISTINCT o.opportunity_id) as opportunities,
    COUNT(DISTINCT CASE WHEN cd.status = 'Won' THEN cd.deal_id END) as won_deals,
    SUM(CASE WHEN cd.status = 'Won' THEN cd.deal_value ELSE 0 END) as revenue,
    AVG(o.amount) as avg_opp_value,
    STRING_AGG(DISTINCT o.customer_id, ', ') as customer_ids
FROM marketing_attribution ma
JOIN campaigns c ON ma.campaign_id = c.campaign_id
JOIN opportunities o ON ma.opportunity_id = o.opportunity_id
LEFT JOIN closed_deals cd ON o.opportunity_id = cd.opportunity_id
WHERE o.sales_rep = 'MY_NAME'
    AND o.created_date >= CURRENT_DATE - 180
GROUP BY c.campaign_name, c.campaign_type
HAVING COUNT(DISTINCT o.opportunity_id) >= 2
ORDER BY revenue DESC, opportunities DESC;
```

---

## 🎯 Product & Cross-Sell

### 14. **"Which products am I selling most successfully?"**
**Analysis**: Product performance by rep
```sql
SELECT 
    p.product_name,
    p.product_category,
    COUNT(CASE WHEN cd.status = 'Won' THEN 1 END) as units_sold,
    COUNT(CASE WHEN cd.status = 'Lost' THEN 1 END) as lost_deals,
    ROUND(COUNT(CASE WHEN cd.status = 'Won' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2) as win_rate,
    SUM(CASE WHEN cd.status = 'Won' THEN cd.deal_value ELSE 0 END) as revenue,
    AVG(CASE WHEN cd.status = 'Won' THEN cd.deal_value END) as avg_deal_size,
    AVG(CASE WHEN cd.status = 'Won' THEN cd.sales_cycle_days END) as avg_sales_cycle
FROM closed_deals cd
JOIN products p ON cd.product_id = p.product_id
WHERE cd.sales_rep = 'MY_NAME'
    AND cd.fiscal_year = 2024
GROUP BY p.product_name, p.product_category
ORDER BY revenue DESC;
```

### 15. **"Which customers should I target for Product X cross-sell?"**
**Analysis**: Cross-sell targets for specific product
```sql
-- Customers who don't have Product X but have similar profiles to those who do
WITH product_x_customers AS (
    SELECT DISTINCT c.industry, c.company_size, c.region
    FROM customers c
    JOIN customer_products cp ON c.customer_id = cp.customer_id