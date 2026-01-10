-- Data Transformation and Cleaning

-- Quality Check: A Primary Key must be unique and not null
-- Check for Nulls or Duplicates in Primary key
-- Expectation: No Results
SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Quality Check: Check for unwanted spaces in string values
-- Expectation: No Results
SELECT cst_firstname FROM bronze.crm_cust_info -- check all columns with String values
WHERE cst_firstname != TRIM(cst_firstname);

-- Quality Check: Check the consistency of values in low cardinality columns
-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info
-- In our DWH we aim to store clear and meaningful values rather than usin abbreviated terms e.g. using Male instead of M
-- In our DWH we use the default value 'n/a' for missing values (nulls)

/*
==================================================================================================
*/
PRINT '> Truncating Table: silver.crm_cust_info <';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '> Inserting Data into: silver.crm_cust_info <';
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
SELECT  
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status, -- Data Normalization & Standardization
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr, 
cst_create_date
FROM (	
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;

/*
==================================================================================================
*/

-- Re-run to check the quality of data in the Silver layer
SELECT cst_id, COUNT(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


SELECT cst_firstname FROM silver.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname);


SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;


SELECT * FROM silver.crm_cust_info;
