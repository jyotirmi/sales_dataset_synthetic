-- =====================================================
-- SNOWFLAKE ROW-LEVEL SECURITY IMPLEMENTATION
-- Scenario: Sales reps see only their data, managers see their team's data
-- =====================================================

-- =====================================================
-- STEP 1: CREATE A MAPPING TABLE FOR USER ROLES AND HIERARCHY
-- =====================================================

-- Create a schema for security objects
CREATE SCHEMA IF NOT EXISTS SALES_ANALYTICS_DEMO.SECURITY;

-- Create user access mapping table
CREATE OR REPLACE TABLE SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING (
    SNOWFLAKE_USER VARCHAR(100),      -- Snowflake username (case-sensitive)
    SALES_REP_NAME VARCHAR(100),      -- Name in the sales_rep column
    USER_ROLE VARCHAR(50),             -- 'REP' or 'MANAGER'
    MANAGER_NAME VARCHAR(100),        -- Manager's name (if applicable)
    CAN_VIEW_TEAM BOOLEAN DEFAULT FALSE,  -- Can view entire team's data
    TEAM_MEMBERS ARRAY,               -- Array of team member names (for managers)
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample user mappings
-- =====================================================
-- FIXED INSERT for Snowflake
-- =====================================================

-- Insert Mark Wong
INSERT INTO SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING 
(SNOWFLAKE_USER, SALES_REP_NAME, USER_ROLE, MANAGER_NAME, CAN_VIEW_TEAM, TEAM_MEMBERS)
VALUES
('MARK_WONG', 'Mark Wong', 'REP', 'Susan Woski', FALSE, NULL);

-- Insert Susan Woski with team members array (using SELECT)
INSERT INTO SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING 
(SNOWFLAKE_USER, SALES_REP_NAME, USER_ROLE, MANAGER_NAME, CAN_VIEW_TEAM, TEAM_MEMBERS)
SELECT 
    'SUSAN_WOSKI', 
    'Susan Woski', 
    'MANAGER', 
    NULL, 
    TRUE, 
    ARRAY_CONSTRUCT('Mark Wong', 'Rep_1', 'Rep_2', 'Rep_3', 'Rep_4', 'Rep_5');

-- Insert additional sales reps
INSERT INTO SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING 
(SNOWFLAKE_USER, SALES_REP_NAME, USER_ROLE, MANAGER_NAME, CAN_VIEW_TEAM, TEAM_MEMBERS)
VALUES
('REP_1', 'Rep_1', 'REP', 'Susan Woski', FALSE, NULL),
('REP_2', 'Rep_2', 'REP', 'Susan Woski', FALSE, NULL),
('REP_3', 'Rep_3', 'REP', 'Susan Woski', FALSE, NULL),
('REP_4', 'Rep_4', 'REP', 'Susan Woski', FALSE, NULL),
('REP_5', 'Rep_5', 'REP', 'Susan Woski', FALSE, NULL);

-- Verify the mapping table
SELECT * FROM SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING;

-- =====================================================
-- STEP 2: CREATE ROW ACCESS POLICY
-- =====================================================

CREATE OR REPLACE ROW ACCESS POLICY SALES_ANALYTICS_DEMO.SECURITY.SALES_REP_ACCESS_POLICY
AS (sales_rep_column VARCHAR) RETURNS BOOLEAN ->
  CASE
    -- Option 1: User is a manager who can view team data
    WHEN EXISTS (
      SELECT 1 
      FROM SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING m
      WHERE UPPER(m.SNOWFLAKE_USER) = CURRENT_USER()
        AND m.IS_ACTIVE = TRUE
        AND m.CAN_VIEW_TEAM = TRUE
        AND (
          -- Manager can see their own data
          sales_rep_column = m.SALES_REP_NAME
          -- Manager can see their team members' data
          OR ARRAY_CONTAINS(sales_rep_column::VARIANT, m.TEAM_MEMBERS)
        )
    ) THEN TRUE
    
    -- Option 2: User is a regular sales rep seeing only their own data
    WHEN EXISTS (
      SELECT 1 
      FROM SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING m
      WHERE UPPER(m.SNOWFLAKE_USER) = CURRENT_USER()
        AND m.IS_ACTIVE = TRUE
        AND sales_rep_column = m.SALES_REP_NAME
    ) THEN TRUE
    
    -- Option 3: User has ACCOUNTADMIN or custom admin role (see everything)
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'SALES_ADMIN') THEN TRUE
    
    -- Default: Deny access
    ELSE FALSE
  END;

-- =====================================================
-- STEP 3: APPLY ROW ACCESS POLICY TO TABLES
-- =====================================================

-- Apply to OPPORTUNITIES table
ALTER TABLE SALES_ANALYTICS_DEMO.SALES_DATA.OPPORTUNITIES
  ADD ROW ACCESS POLICY SALES_ANALYTICS_DEMO.SECURITY.SALES_REP_ACCESS_POLICY 
  ON (sales_rep);

-- Apply to CLOSED_DEALS table
ALTER TABLE SALES_ANALYTICS_DEMO.SALES_DATA.CLOSED_DEALS
  ADD ROW ACCESS POLICY SALES_ANALYTICS_DEMO.SECURITY.SALES_REP_ACCESS_POLICY 
  ON (sales_rep);

-- Apply to CUSTOMERS table (on account_owner column)
ALTER TABLE SALES_ANALYTICS_DEMO.SALES_DATA.CUSTOMERS
  ADD ROW ACCESS POLICY SALES_ANALYTICS_DEMO.SECURITY.SALES_REP_ACCESS_POLICY 
  ON (account_owner);

-- =====================================================
-- STEP 4: CREATE ROLES FIRST (BEFORE USERS)
-- =====================================================

-- Create roles (run as ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;


-- Create roles
CREATE ROLE IF NOT EXISTS SALES_REP_ROLE
  COMMENT = 'Role for sales representatives - can view only their own data';

CREATE ROLE IF NOT EXISTS SALES_MANAGER_ROLE
  COMMENT = 'Role for sales managers - can view their team data';

CREATE ROLE IF NOT EXISTS SALES_ADMIN
  COMMENT = 'Role for sales administrators - can view all sales data';

-- Create role hierarchy (managers inherit rep permissions)
GRANT ROLE SALES_REP_ROLE TO ROLE SALES_MANAGER_ROLE;
GRANT ROLE SALES_MANAGER_ROLE TO ROLE SALES_ADMIN;

-- Grant roles to SYSADMIN for proper hierarchy
GRANT ROLE SALES_REP_ROLE TO ROLE SYSADMIN;
GRANT ROLE SALES_MANAGER_ROLE TO ROLE SYSADMIN;
GRANT ROLE SALES_ADMIN TO ROLE SYSADMIN;

-- =====================================================
-- STEP 5: CREATE USERS IN SNOWFLAKE
-- =====================================================

-- Create Mark Wong user
CREATE USER IF NOT EXISTS MARK_WONG
  PASSWORD = 'MarkWong2024!'
  LOGIN_NAME = 'MARK_WONG'
  DISPLAY_NAME = 'Mark Wong'
  FIRST_NAME = 'Mark'
  LAST_NAME = 'Wong'
  EMAIL = 'mark.wong@company.com'
  DEFAULT_ROLE = SALES_REP_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH
  DEFAULT_NAMESPACE = 'SALES_ANALYTICS_DEMO.SALES_DATA'
  MUST_CHANGE_PASSWORD = TRUE
  COMMENT = 'Sales Representative - Mark Wong';

-- Create Susan Woski user
CREATE USER IF NOT EXISTS SUSAN_WOSKI
  PASSWORD = 'SusanWoski2024!'
  LOGIN_NAME = 'SUSAN_WOSKI'
  DISPLAY_NAME = 'Susan Woski'
  FIRST_NAME = 'Susan'
  LAST_NAME = 'Woski'
  EMAIL = 'susan.woski@company.com'
  DEFAULT_ROLE = SALES_MANAGER_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH
  DEFAULT_NAMESPACE = 'SALES_ANALYTICS_DEMO.SALES_DATA'
  MUST_CHANGE_PASSWORD = TRUE
  COMMENT = 'Sales Manager - Susan Woski (Manages Mark Wong and team)';

-- Create additional sample users for the team
CREATE USER IF NOT EXISTS REP_1
  PASSWORD = 'Rep1Password!'
  LOGIN_NAME = 'REP_1'
  DISPLAY_NAME = 'Sales Rep 1'
  EMAIL = 'rep1@company.com'
  DEFAULT_ROLE = SALES_REP_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH
  DEFAULT_NAMESPACE = 'SALES_ANALYTICS_DEMO.SALES_DATA'
  MUST_CHANGE_PASSWORD = TRUE
  COMMENT = 'Sales Representative - Rep 1';

CREATE USER IF NOT EXISTS REP_2
  PASSWORD = 'Rep2Password!'
  LOGIN_NAME = 'REP_2'
  DISPLAY_NAME = 'Sales Rep 2'
  EMAIL = 'rep2@company.com'
  DEFAULT_ROLE = SALES_REP_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH
  DEFAULT_NAMESPACE = 'SALES_ANALYTICS_DEMO.SALES_DATA'
  MUST_CHANGE_PASSWORD = TRUE
  COMMENT = 'Sales Representative - Rep 2';

-- Verify users were created
SHOW USERS LIKE '%WONG%';
SHOW USERS LIKE '%WOSKI%';
SHOW USERS LIKE 'REP_%';

-- =====================================================
-- STEP 5: CREATE AND ASSIGN ROLES
-- =====================================================

-- Create roles
CREATE ROLE IF NOT EXISTS SALES_REP_ROLE;
CREATE ROLE IF NOT EXISTS SALES_MANAGER_ROLE;
CREATE ROLE IF NOT EXISTS SALES_ADMIN;

-- Grant database and schema usage
GRANT USAGE ON DATABASE SALES_ANALYTICS_DEMO TO ROLE SALES_REP_ROLE;
GRANT USAGE ON SCHEMA SALES_ANALYTICS_DEMO.SALES_DATA TO ROLE SALES_REP_ROLE;
GRANT USAGE ON SCHEMA SALES_ANALYTICS_DEMO.SECURITY TO ROLE SALES_REP_ROLE;

GRANT USAGE ON DATABASE SALES_ANALYTICS_DEMO TO ROLE SALES_MANAGER_ROLE;
GRANT USAGE ON SCHEMA SALES_ANALYTICS_DEMO.SALES_DATA TO ROLE SALES_MANAGER_ROLE;
GRANT USAGE ON SCHEMA SALES_ANALYTICS_DEMO.SECURITY TO ROLE SALES_MANAGER_ROLE;

-- Grant table access (SELECT only for reps and managers)
GRANT SELECT ON ALL TABLES IN SCHEMA SALES_ANALYTICS_DEMO.SALES_DATA TO ROLE SALES_REP_ROLE;
GRANT SELECT ON TABLE SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING TO ROLE SALES_REP_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA SALES_ANALYTICS_DEMO.SALES_DATA TO ROLE SALES_MANAGER_ROLE;
GRANT SELECT ON TABLE SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING TO ROLE SALES_MANAGER_ROLE;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SALES_REP_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SALES_MANAGER_ROLE;

-- Assign roles to users
GRANT ROLE SALES_REP_ROLE TO USER MARK_WONG;
GRANT ROLE SALES_MANAGER_ROLE TO USER SUSAN_WOSKI;

-- For managers, also grant rep role so they have all permissions
GRANT ROLE SALES_REP_ROLE TO ROLE SALES_MANAGER_ROLE;

-- Grant roles to SYSADMIN for role hierarchy
GRANT ROLE SALES_REP_ROLE TO ROLE SYSADMIN;
GRANT ROLE SALES_MANAGER_ROLE TO ROLE SYSADMIN;

-- =====================================================
-- STEP 6: TESTING THE ROW-LEVEL SECURITY
-- =====================================================

-- Test as Mark Wong (should only see his opportunities)
USE ROLE SALES_REP_ROLE;
USE WAREHOUSE COMPUTE_WH;

-- Switch to Mark Wong user context (simulated)
-- In practice, Mark would log in as MARK_WONG
SELECT CURRENT_USER(), CURRENT_ROLE();

-- Query opportunities - Mark should only see his own
SELECT 
    opportunity_id,
    customer_id,
    sales_rep,
    amount,
    stage
FROM SALES_ANALYTICS_DEMO.SALES_DATA.OPPORTUNITIES
WHERE sales_rep = 'Mark Wong'
LIMIT 10;

-- Count Mark's opportunities
SELECT COUNT(*) as mark_opp_count
FROM SALES_ANALYTICS_DEMO.SALES_DATA.OPPORTUNITIES;

-- Test as Susan Woski (should see her data + her team's data)
-- Switch to Susan's context
USE ROLE SALES_MANAGER_ROLE;

-- Query opportunities - Susan should see her team's data
SELECT 
    sales_rep,
    COUNT(*) as opp_count,
    SUM(amount) as total_amount
FROM SALES_ANALYTICS_DEMO.SALES_DATA.OPPORTUNITIES
GROUP BY sales_rep
ORDER BY opp_count DESC;

-- Verify Susan can see Mark's data
SELECT 
    opportunity_id,
    customer_id,
    sales_rep,
    amount,
    stage
FROM SALES_ANALYTICS_DEMO.SALES_DATA.OPPORTUNITIES
WHERE sales_rep = 'Mark Wong'
LIMIT 10;

-- =====================================================
-- STEP 7: ADMINISTRATIVE QUERIES
-- =====================================================

-- View all row access policies (run as ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;

SHOW ROW ACCESS POLICIES IN SCHEMA SALES_ANALYTICS_DEMO.SECURITY;

-- See which tables have policies applied
SELECT 
    policy_name,
    policy_schema,
    ref_entity_name as table_name,
    ref_entity_domain as object_type,
    ref_column_name as filtered_column,
    policy_status
FROM TABLE(
    INFORMATION_SCHEMA.POLICY_REFERENCES(
        POLICY_NAME => 'SALES_ANALYTICS_DEMO.SECURITY.SALES_REP_ACCESS_POLICY'
    )
);

-- =====================================================
-- STEP 8: MAINTENANCE - ADD/REMOVE USERS
-- =====================================================

-- Add a new sales rep
INSERT INTO SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING 
(SNOWFLAKE_USER, SALES_REP_NAME, USER_ROLE, MANAGER_NAME, CAN_VIEW_TEAM)
VALUES ('REP_6', 'Rep_6', 'REP', 'Susan Woski', FALSE);

-- Update Susan's team to include the new rep
UPDATE SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING
SET TEAM_MEMBERS = ARRAY_CONSTRUCT('Mark Wong', 'Rep_1', 'Rep_2', 'Rep_3', 'Rep_4', 'Rep_5', 'Rep_6')
WHERE SNOWFLAKE_USER = 'SUSAN_WOSKI';

-- Deactivate a user
UPDATE SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING
SET IS_ACTIVE = FALSE
WHERE SNOWFLAKE_USER = 'REP_6';

-- =====================================================
-- STEP 9: REMOVE ROW ACCESS POLICY (IF NEEDED)
-- =====================================================

-- Remove policy from a table
-- ALTER TABLE SALES_ANALYTICS_DEMO.SALES_DATA.OPPORTUNITIES
--   DROP ROW ACCESS POLICY SALES_ANALYTICS_DEMO.SECURITY.SALES_REP_ACCESS_POLICY;

-- Drop the policy entirely
-- DROP ROW ACCESS POLICY IF EXISTS SALES_ANALYTICS_DEMO.SECURITY.SALES_REP_ACCESS_POLICY;

-- =====================================================
-- ALTERNATIVE: SIMPLIFIED VERSION USING MAPPING TABLE
-- =====================================================

-- If you want a simpler version without arrays:
/*
CREATE OR REPLACE ROW ACCESS POLICY SALES_ANALYTICS_DEMO.SECURITY.SALES_REP_ACCESS_POLICY_SIMPLE
AS (sales_rep_column VARCHAR) RETURNS BOOLEAN ->
  CASE
    -- Check if current user can see this sales rep's data
    WHEN EXISTS (
      SELECT 1 
      FROM SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING m
      WHERE UPPER(m.SNOWFLAKE_USER) = CURRENT_USER()
        AND m.IS_ACTIVE = TRUE
        AND (
          -- User is viewing their own data
          sales_rep_column = m.SALES_REP_NAME
          -- OR user is a manager of this sales rep
          OR (m.CAN_VIEW_TEAM = TRUE AND sales_rep_column IN (
            SELECT SALES_REP_NAME 
            FROM SALES_ANALYTICS_DEMO.SECURITY.USER_ACCESS_MAPPING
            WHERE MANAGER_NAME = m.SALES_REP_NAME
          ))
        )
    ) THEN TRUE
    
    -- Admin roles see everything
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'SALES_ADMIN') THEN TRUE
    
    ELSE FALSE
  END;
*/

-- =====================================================
-- NOTES AND BEST PRACTICES
-- =====================================================

/*
IMPORTANT NOTES:

1. USER NAMING: Snowflake usernames are case-insensitive but stored in uppercase
   - Use UPPER() or match the case in your mapping table

2. PERFORMANCE: Row access policies are evaluated on every query
   - Keep the policy logic simple
   - Index the mapping table if it grows large
   - Consider caching frequently accessed mappings

3. TESTING: Always test as different users before deploying
   - Use USE ROLE to test different role contexts
   - Create test users with limited permissions

4. SECURITY: 
   - Mapping table should only be writable by admins
   - Consider using secure views for additional security layers
   - Audit changes to the mapping table

5. SSO INTEGRATION: 
   - In production, use SSO/SAML for authentication
   - Map SSO usernames to Snowflake users
   - Sync with your HR/Identity system

6. HIERARCHY: For complex hierarchies, consider:
   - Recursive CTEs for multi-level management
   - Separate hierarchy table
   - Integration with HR systems

7. MONITORING:
   - Track who accesses what data
   - Monitor policy performance impact
   - Review access patterns regularly
*/