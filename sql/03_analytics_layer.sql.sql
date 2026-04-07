-- =====================================================
-- STEP: CREATE EMPLOYER TABLE
-- Layer: ANALYTICS_LAYER
-- Purpose:
-- - Final employer-level analytics table
-- - Used for dashboards (top companies, trends, salary)
-- =====================================================

CREATE OR REPLACE TABLE H1B_ANALYSIS.ANALYTICS_LAYER.EMPLOYER AS

SELECT
    employer_name,
    naics_code,
    employer_state,
    year,
    quarter,

    -- Total applications
    COUNT(*) AS total_cases,

    -- Total workers requested
    SUM(total_workers) AS total_workers,

    -- Salary insights
    AVG(wage_from) AS avg_salary,
    MAX(wage_from) AS max_salary,

    -- Approval rate
    ROUND(
        100 * SUM(CASE WHEN case_status = 'CERTIFIED' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS approval_rate

FROM H1B_ANALYSIS.CLEAN_LAYER.CLEAN_H1B_REFINED

GROUP BY employer_name, naics_code, employer_state, year, quarter;

select * from H1B_ANALYSIS.ANALYTICS_LAYER.EMPLOYER

-- =====================================================
-- STEP: CREATE JOB TABLE
-- Layer: ANALYTICS_LAYER
-- Purpose:
-- - Final job-level analytics table
-- - Used for role demand & salary dashboards
-- =====================================================

CREATE OR REPLACE TABLE H1B_ANALYSIS.ANALYTICS_LAYER.JOB AS

SELECT
    job_title,
    soc_code,
    soc_title,
    year,
    quarter,

    -- Total applications
    COUNT(*) AS total_cases,

    -- Total workers requested
    SUM(total_workers) AS total_workers,

    -- Salary insights
    AVG(wage_from) AS avg_salary,
    MAX(wage_from) AS max_salary,

    -- Approval rate
    ROUND(
        100 * SUM(CASE WHEN case_status = 'CERTIFIED' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS approval_rate

FROM H1B_ANALYSIS.CLEAN_LAYER.CLEAN_H1B_REFINED

GROUP BY job_title, soc_code, soc_title, year, quarter;

select * from H1B_ANALYSIS.ANALYTICS_LAYER.JOB

-- =====================================================
-- STEP: CREATE LOCATION TABLE
-- Layer: ANALYTICS_LAYER
-- Purpose:
-- - Final location-level analytics table
-- - Used for map and geographic dashboards
-- =====================================================

CREATE OR REPLACE TABLE H1B_ANALYSIS.ANALYTICS_LAYER.LOCATION AS

SELECT
    worksite_state,
    worksite_city,
    year,
    quarter,

    -- Total applications
    COUNT(*) AS total_cases,

    -- Total workers requested
    SUM(total_workers) AS total_workers,

    -- Salary insights
    AVG(wage_from) AS avg_salary,
    MAX(wage_from) AS max_salary,

    -- Approval rate
    ROUND(
        100 * SUM(CASE WHEN case_status = 'CERTIFIED' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS approval_rate

FROM H1B_ANALYSIS.CLEAN_LAYER.CLEAN_H1B_REFINED

GROUP BY worksite_state, worksite_city, year, quarter;


select * from H1B_ANALYSIS.ANALYTICS_LAYER.LOCATION 


-- =====================================================
-- STEP: CREATE SALARY TABLE
-- Layer: ANALYTICS_LAYER
-- Purpose:
-- - Compare offered wage vs prevailing wage
-- - Enable salary gap analysis
-- =====================================================

CREATE OR REPLACE TABLE H1B_ANALYSIS.ANALYTICS_LAYER.SALARY AS

SELECT
    employer_name,
    job_title,
    worksite_state,
    year,
    quarter,

    -- Salary values
    wage_from,
    prevailing_wage,

    -- Difference between offered and prevailing wage
    (wage_from - prevailing_wage) AS wage_difference,

    -- Categorize salary vs market
    CASE
        WHEN wage_from > prevailing_wage THEN 'ABOVE_PREVAILING'
        WHEN wage_from < prevailing_wage THEN 'BELOW_PREVAILING'
        ELSE 'EQUAL'
    END AS wage_category

FROM H1B_ANALYSIS.CLEAN_LAYER.CLEAN_H1B_REFINED
WHERE wage_from IS NOT NULL
AND prevailing_wage IS NOT NULL;

select * from H1B_ANALYSIS.ANALYTICS_LAYER.SALARY

-- =====================================================
-- EXPORT TABLE TO CSV (Snowflake → Stage)
-- =====================================================

COPY INTO @~/employer_data
FROM H1B_ANALYSIS.ANALYTICS_LAYER.EMPLOYER
FILE_FORMAT = (
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
HEADER = TRUE
SINGLE = TRUE;

COPY INTO @~/job_data
FROM H1B_ANALYSIS.ANALYTICS_LAYER.JOB
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
HEADER = TRUE
SINGLE = TRUE;

COPY INTO @~/location_data
FROM H1B_ANALYSIS.ANALYTICS_LAYER.LOCATION
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
HEADER = TRUE
SINGLE = TRUE;

COPY INTO @~/salary_data
FROM H1B_ANALYSIS.ANALYTICS_LAYER.SALARY
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
HEADER = TRUE
SINGLE = TRUE;

LIST @~/employer_data;
LIST @~/job_data
LIST @~/location_data
LIST @~/salary_data

LIST @~;