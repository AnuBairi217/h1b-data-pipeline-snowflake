--Creating CREATE CLEAN TABLE
CREATE OR REPLACE TABLE H1B_ANALYSIS.CLEAN_LAYER.CLEAN_H1B_BASE AS

SELECT
    TRIM("Case Number") AS case_number,
    UPPER("Case Status") AS case_status,
    TRY_TO_DATE("Received Date") AS received_date,
    TRY_TO_DATE("Decision Date") AS decision_date,
    UPPER("Visa Class") AS visa_class,

    UPPER("Employer Name") AS employer_name,
    "NAICS Code" AS naics_code,
    UPPER("Employer State") AS employer_state,

    UPPER("Job Title") AS job_title,
    "SOC Code" AS soc_code,
    "SOC Title" AS soc_title,

    UPPER("Worksite City") AS worksite_city,
    UPPER("Worksite State") AS worksite_state,

    TRY_TO_NUMBER(REPLACE(REPLACE("Wage Rate of Pay From", '$',''), ',', '')) AS wage_from,
    TRY_TO_NUMBER(REPLACE(REPLACE("Wage Rate of Pay To", '$',''), ',', '')) AS wage_to,
    "Wage Unit of Pay" AS wage_unit,

    TRY_TO_NUMBER(REPLACE(REPLACE("Prevailing Wage", '$',''), ',', '')) AS prevailing_wage,

    "Total Worker Positions" AS total_workers,

    "New Employment",
    "Continued Employment",
    "Change Previous Employment",
    "Change Employer",

    2025 AS year,
    'Q2' AS quarter

FROM H1B_ANALYSIS.RAW_LAYER.RAW_H1B_2025_Q2

UNION ALL

SELECT
    TRIM("Case Number"),
    UPPER("Case Status"),
    TRY_TO_DATE("Received Date"),
    TRY_TO_DATE("Decision Date"),
    UPPER("Visa Class"),

    UPPER("Employer Name"),
    "NAICS Code",
    UPPER("Employer State"),

    UPPER("Job Title"),
    "SOC Code",
    "SOC Title",

    UPPER("Worksite City"),
    UPPER("Worksite State"),

    TRY_TO_NUMBER(REPLACE(REPLACE("Wage Rate of Pay From", '$',''), ',', '')),
    TRY_TO_NUMBER(REPLACE(REPLACE("Wage Rate of Pay To", '$',''), ',', '')),
    "Wage Unit of Pay",

    TRY_TO_NUMBER(REPLACE(REPLACE("Prevailing Wage", '$',''), ',', '')),

    "Total Worker Positions",

    "New Employment",
    "Continued Employment",
    "Change Previous Employment",
    "Change Employer",

    2026 AS year,
    'Q1' AS quarter

FROM H1B_ANALYSIS.RAW_LAYER.RAW_H1B_2026_Q1;

DESC TABLE H1B_ANALYSIS.RAW_LAYER.RAW_H1B_2025_Q2;


SELECT * 
FROM H1B_ANALYSIS.RAW_LAYER.RAW_H1B_2025_Q2
LIMIT 5;

SELECT * 
FROM H1B_ANALYSIS.RAW_LAYER.RAW_H1B_2026_Q1
LIMIT 5;

-- =====================================================
-- STEP 1A: CLEAN 2025 Q2 RAW DATA
-- Purpose:
-- - Standardize column names (lowercase, underscore format)
-- - Convert data types (dates, wages)
-- - Normalize text fields (UPPER)
-- - Add year & quarter for analysis
-- =====================================================

CREATE OR REPLACE TABLE CLEAN_LAYER.CLEAN_2025_Q2 AS
SELECT
    TRIM(CASE_NUMBER) AS case_number,
    UPPER(CASE_STATUS) AS case_status,
    TRY_TO_DATE(RECEIVED_DATE) AS received_date,
    TRY_TO_DATE(DECISION_DATE) AS decision_date,
    UPPER(VISA_CLASS) AS visa_class,

    UPPER(EMPLOYER_NAME) AS employer_name,

    -- FIX: force NAICS_CODE to STRING
    TO_VARCHAR(NAICS_CODE) AS naics_code,

    UPPER(EMPLOYER_STATE) AS employer_state,

    UPPER(JOB_TITLE) AS job_title,
    SOC_CODE,
    SOC_TITLE,

    UPPER(WORKSITE_CITY) AS worksite_city,
    UPPER(WORKSITE_STATE) AS worksite_state,

    TRY_TO_NUMBER(REPLACE(REPLACE(WAGE_RATE_OF_PAY_FROM, '$',''), ',', '')) AS wage_from,
    TRY_TO_NUMBER(REPLACE(REPLACE(WAGE_RATE_OF_PAY_TO, '$',''), ',', '')) AS wage_to,
    WAGE_UNIT_OF_PAY AS wage_unit,

    TRY_TO_NUMBER(REPLACE(REPLACE(PREVAILING_WAGE, '$',''), ',', '')) AS prevailing_wage,

    TOTAL_WORKER_POSITIONS AS total_workers,

    NEW_EMPLOYMENT,
    CONTINUED_EMPLOYMENT,
    CHANGE_PREVIOUS_EMPLOYMENT,
    CHANGE_EMPLOYER,

    2025 AS year,
    'Q2' AS quarter

FROM RAW_LAYER.RAW_H1B_2025_Q2;

-- =====================================================
-- STEP 1B: CLEAN 2026 Q1 RAW DATA
-- Purpose:
-- - Handle column names with spaces using quotes
-- - Standardize structure to match 2025 dataset
-- - Prepare for union
-- =====================================================
-- STEP 1B: CLEAN 2026 Q1 RAW DATA (FIXED COLUMN NAMES)
-- =====================================================
-- =====================================================

CREATE OR REPLACE TABLE CLEAN_LAYER.CLEAN_2026_Q1 AS
SELECT
    TRIM(CASE_NUMBER) AS case_number,
    UPPER(CASE_STATUS) AS case_status,
    TRY_TO_DATE(RECEIVED_DATE) AS received_date,
    TRY_TO_DATE(DECISION_DATE) AS decision_date,
    UPPER(VISA_CLASS) AS visa_class,

    UPPER(EMPLOYER_NAME) AS employer_name,

    -- FIX: force NAICS_CODE to STRING
    TO_VARCHAR(NAICS_CODE) AS naics_code,

    UPPER(EMPLOYER_STATE) AS employer_state,

    UPPER(JOB_TITLE) AS job_title,
    SOC_CODE,
    SOC_TITLE,

    UPPER(WORKSITE_CITY) AS worksite_city,
    UPPER(WORKSITE_STATE) AS worksite_state,

    TRY_TO_NUMBER(REPLACE(REPLACE(WAGE_RATE_OF_PAY_FROM, '$',''), ',', '')) AS wage_from,
    TRY_TO_NUMBER(REPLACE(REPLACE(WAGE_RATE_OF_PAY_TO, '$',''), ',', '')) AS wage_to,
    WAGE_UNIT_OF_PAY AS wage_unit,

    TRY_TO_NUMBER(REPLACE(REPLACE(PREVAILING_WAGE, '$',''), ',', '')) AS prevailing_wage,

    TOTAL_WORKER_POSITIONS AS total_workers,

    NEW_EMPLOYMENT,
    CONTINUED_EMPLOYMENT,
    CHANGE_PREVIOUS_EMPLOYMENT,
    CHANGE_EMPLOYER,

    2026 AS year,
    'Q1' AS quarter

FROM RAW_LAYER.RAW_H1B_2026_Q1;

-- =====================================================
-- STEP 2: DATA QUALITY CLEANING
-- Purpose:
-- - Remove invalid / junk records
-- - Ensure important fields are usable
-- - Prepare dataset for analytics
-- =====================================================

-- =====================================================
-- CREATE BASE TABLE (COMBINED DATA)
-- Purpose:
-- - Merge both cleaned datasets
-- - This becomes the foundation for all further steps
-- =====================================================

CREATE OR REPLACE TABLE CLEAN_LAYER.CLEAN_H1B_BASE AS

SELECT * FROM CLEAN_LAYER.CLEAN_2025_Q2
UNION ALL
SELECT * FROM CLEAN_LAYER.CLEAN_2026_Q1;

-- =====================================================
-- STEP: CREATE REFINED CLEAN TABLE
-- Purpose:
-- - Remove junk / invalid records
-- - Keep only high-quality data for analytics
-- =====================================================

CREATE OR REPLACE TABLE H1B_ANALYSIS.CLEAN_LAYER.CLEAN_H1B_REFINED AS

SELECT *
FROM H1B_ANALYSIS.CLEAN_LAYER.CLEAN_H1B_BASE
WHERE 1=1

-- Remove invalid records
AND case_number IS NOT NULL

-- Keep meaningful statuses only
AND case_status IN ('CERTIFIED', 'DENIED', 'WITHDRAWN', 'CERTIFIED-WITHDRAWN')

-- Remove missing key business fields
AND employer_name IS NOT NULL
AND job_title IS NOT NULL
AND worksite_state IS NOT NULL

-- Salary sanity check
AND wage_from IS NOT NULL
AND wage_from > 0
AND wage_from < 1000000;

SELECT COUNT(*) 
FROM H1B_ANALYSIS.CLEAN_LAYER.CLEAN_H1B_REFINED;