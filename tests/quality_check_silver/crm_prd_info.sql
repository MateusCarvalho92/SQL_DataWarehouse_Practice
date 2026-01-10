-- Data Transformation and Cleaning

-- Quality Check: A Primary Key must be unique and not null
-- Check for Nulls or Duplicates in Primary key
-- Expectation: No Results
SELECT prd_id, COUNT(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2; -- double check to join data from this table with the prd_key substring from crm_prd_info table 
SELECT sls_prd_key FROM bronze.crm_sales_details; -- same as above

-- Quality Check: Check for unwanted spaces in string values
-- Expectation: No Results
SELECT prd_nm FROM bronze.crm_prd_info -- check all columns with String values
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Quality Check: Check the consistency of values in low cardinality columns
-- Data Standardization & Consistency
SELECT DISTINCT prd_line FROM bronze.crm_prd_info;

-- Check for Invalid Date Orders
SELECT * FROM bronze.crm_prd_info -- End date must not be earlier than the start date
WHERE prd_end_dt < prd_start_dt;
-- Solution 1: switch end_dt with start_dt
-- Solution 2: Derive the end_dt from the start_date - end_dt = start_dt of the "Next" Record -1
-- REMINDER: Validate with an expert from the source system
-- Testing Solution 2:
SELECT prd_id,prd_key,prd_nm,prd_start_dt,prd_end_dt, 
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

/*
==================================================================================================
*/
PRINT '> Truncating Table: silver.crm_prd_info <';
TRUNCATE TABLE silver.crm_prd_info;
PRINT '> Inserting Data into: silver.crm_prd_info <';
INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT 
prd_id,
-- Derived Columns from prd_key > cat_id and prd_key
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,	-- Substring() Extracts a specific part of a string value
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Len() returns the number of chars in a string
prd_nm,
-- Handling NULLs
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line)) 
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END prd_line,
-- Data Normalization and Transformation / Data Enrichment
CAST(prd_start_dt  AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;
/*	WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') IN - or NOT IN to check if it matches
	(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)
	WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN
	(SELECT sls_prd_key FROM bronze.crm_sales_details) */

/*
==================================================================================================
*/

-- Re-run to check the quality of data in the Silver layer
SELECT prd_id, COUNT(*) FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


SELECT DISTINCT id FROM silver.erp_px_cat_g1v2;
SELECT sls_prd_key FROM silver.crm_sales_details;


SELECT prd_nm FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


SELECT prd_cost FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


SELECT DISTINCT prd_line FROM silver.crm_prd_info;


SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


SELECT * FROM silver.crm_prd_info;
