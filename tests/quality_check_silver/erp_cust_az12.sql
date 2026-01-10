-- Identify Out-of-Range Dates
SELECT DISTINCT bdate FROM bronze.erp_cust_az12
WHERE bdate < '1920-01-01'OR bdate > GETDATE()
-- REMINDER: Check with a source expert
-- In this case, only replacing the extremes

-- Data Standardization & Consistencty
SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END gen FROM bronze.erp_cust_az12;


/*
==================================================================================================
*/

PRINT '> Truncating Table: silver.erp_cust_az12 <';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '> Inserting Data into: silver.erp_cust_az12 <';
INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
	 ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12;
-- WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))		-- To check Join
--  	      ELSE cid END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

/*
==================================================================================================
*/


-- Re-run to check the quality of data in the Silver layer
SELECT DISTINCT bdate FROM silver.erp_cust_az12
WHERE bdate < '1920-01-01'OR bdate > GETDATE()


SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END gen FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;
