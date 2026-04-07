/* 
PROJECT: H1B Sponsorship Data Analysis
AUTHOR: Anu Bairi
TOOLS: Snowflake, SQL
DATA SOURCE: U.S Department of Labor H1B Disclosure Dataset

PROJECT OBJECTIVE:
Analyze H1B visa sponsorship trends including:

- Top sponsoring companies
- Most common job titles
- Salary distribution
- Approval vs denial rates
- Geographic trends

DATA PIPELINE STRUCTURE:

RAW_LAYER        → Raw ingested datasets
CLEAN_LAYER      → Cleaned and standardized datasets
ANALYTICS_LAYER  → Analytical tables and insights

*/

/* =====================================
SECTION 1 — DATABASE SETUP
===================================== */

CREATE DATABASE H1B_ANALYSIS;

USE DATABASE H1B_ANALYSIS;

CREATE SCHEMA RAW_LAYER;

CREATE SCHEMA CLEAN_LAYER;

CREATE SCHEMA ANALYTICS_LAYER;

/* =====================================
SECTION 2 — RAW DATA INSPECTION
===================================== */

-- Raw dataset loaded from Department of Labor H1B disclosure data
-- Year: 2026
-- Quarter: Q1

SELECT *
FROM RAW_LAYER.RAW_H1B_2026_Q1
LIMIT 10;
SHOW SCHEMAS;
SHOW DATABASES;
H1B_ANALYSIS.RAW_LAYER

/* =====================================
SECTION 3 — DATA EXPLORATION
===================================== */
SELECT COUNT(*)
FROM RAW_LAYER.RAW_H1B_2026_Q1;

DESC TABLE RAW_LAYER.RAW_H1B_2026_Q1;

-- Count total number of rows in dataset
SELECT COUNT(*) AS total_records
FROM RAW_LAYER.RAW_H1B_2026_Q1;

-- Preview first few rows
SELECT *
FROM RAW_LAYER.RAW_H1B_2026_Q1
LIMIT 20;

-- Distribution of petition statuses

SELECT
CASE_STATUS,
COUNT(*) AS total_cases
FROM RAW_LAYER.RAW_H1B_2026_Q1
GROUP BY CASE_STATUS
ORDER BY total_cases DESC;

-- Identify visa classes present

SELECT
VISA_CLASS,
COUNT(*) AS total_cases
FROM RAW_LAYER.RAW_H1B_2026_Q1
GROUP BY VISA_CLASS
ORDER BY total_cases DESC;

-- Identify most common job titles

SELECT
JOB_TITLE,
COUNT(*) AS total_positions
FROM RAW_LAYER.RAW_H1B_2026_Q1
GROUP BY JOB_TITLE
ORDER BY total_positions DESC;
-- Insight: Technology roles dominate H1B sponsorship demand

-- Companies sponsoring the most H1B visas

SELECT
EMPLOYER_NAME,
COUNT(*) AS total_applications
FROM RAW_LAYER.RAW_H1B_2026_Q1
GROUP BY EMPLOYER_NAME
ORDER BY total_applications desc
limit 20;
-- Top 5 companies: Amazon.com Services LLC, Apple Inc, Ernst & Young U.S. LLP, COGNIZANT TECHNOLOGY SOLUTIONS US CORP


-- Identify states with highest H1B employment demand

SELECT
WORKSITE_STATE,
COUNT(*) AS total_positions
FROM RAW_LAYER.RAW_H1B_2026_Q1
GROUP BY WORKSITE_STATE
ORDER BY total_positions DESC;

-- Average salary offered by job title

SELECT
JOB_TITLE,
AVG(WAGE_RATE_OF_PAY_FROM) AS avg_salary
FROM RAW_LAYER.RAW_H1B_2026_Q1
GROUP BY JOB_TITLE
ORDER BY avg_salary DESC
LIMIT 10;

SELECT
    JOB_TITLE,
    AVG(
        TO_NUMBER(
            REPLACE(REPLACE(WAGE_RATE_OF_PAY_FROM,'$',''),',','')
        )
    ) AS avg_salary
FROM RAW_LAYER.RAW_H1B_2026_Q1
GROUP BY JOB_TITLE
ORDER BY avg_salary DESC
LIMIT 10;

SELECT WAGE_RATE_OF_PAY_FROM
FROM RAW_LAYER.RAW_H1B_2026_Q1
LIMIT 20;
/* ======================================================
SECTION 4 — DATA CLEANING
Objective:
Transform the raw dataset into a clean analytical dataset
by selecting relevant columns and filtering useful records.

Source Table:
RAW_LAYER.RAW_H1B_2026_Q1

Target Table:
CLEAN_LAYER.H1B_CLEAN_2026
====================================================== */

-- Create a clean dataset with only relevant columns

CREATE OR REPLACE TABLE CLEAN_LAYER.H1B_CLEAN_2026 AS
SELECT
CASE_NUMBER,
CASE_STATUS,
RECEIVED_DATE,
DECISION_DATE,
VISA_CLASS,

EMPLOYER_NAME,
EMPLOYER_CITY,
EMPLOYER_STATE,

JOB_TITLE,
SOC_TITLE,

WORKSITE_CITY,
WORKSITE_STATE,

TO_NUMBER(REPLACE(REPLACE(WAGE_RATE_OF_PAY_FROM,'$',''),',','')) AS WAGE,
WAGE_UNIT_OF_PAY,
NAICS_CODE

FROM CLEAN_LAYER.H1B_CLEAN_2026;

SELECT *
FROM CLEAN_LAYER.H1B_CLEAN_2026
LIMIT 20;

SELECT *
FROM CLEAN_LAYER.H1B_CLEAN_2026
LIMIT 20;

-- Keep only H1B visa cases
CREATE OR REPLACE TABLE CLEAN_LAYER.H1B_CLEAN_2026 AS
SELECT *
FROM CLEAN_LAYER.H1B_CLEAN_2026
WHERE VISA_CLASS = 'H-1B';

SELECT COUNT(*) FROM CLEAN_LAYER.H1B_CLEAN_2026;
DESC TABLE CLEAN_LAYER.H1B_CLEAN_2026;

/* =====================================
CHECK NULL VALUES IN KEY COLUMNS
===================================== */
SELECT
COUNT(*) AS total_rows,
COUNT(CASE_NUMBER) AS case_number_count,
COUNT(EMPLOYER_NAME) AS employer_count,
COUNT(JOB_TITLE) AS job_title_count,
COUNT(WORKSITE_STATE) AS worksite_state_count,
COUNT(WAGE) AS wage_count
FROM CLEAN_LAYER.H1B_CLEAN_2026;

SELECT *
FROM CLEAN_LAYER.H1B_CLEAN_2026
WHERE JOB_TITLE IS NULL;

/* =====================================
SECTION 4.2 — DUPLICATE CHECK
Check if any CASE_NUMBER appears more than once
===================================== */
SELECT
CASE_NUMBER,
COUNT(*) AS duplicate_count
FROM CLEAN_LAYER.H1B_CLEAN_2026
GROUP BY CASE_NUMBER
HAVING COUNT(*) > 1;

