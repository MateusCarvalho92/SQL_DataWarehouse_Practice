-- To check if info match to join the tables
SELECT cst_key FROM silver.crm_cust_info;

-- Quality Check: Check the consistency of values in low cardinality columns
-- Data Standardization & Consistency
SELECT DISTINCT cntry AS old_cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END cntry FROM bronze.erp_loc_a101;


/*
==================================================================================================
*/

PRINT '> Truncating Table: silver.erp_loc_a101 <';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '> Inserting Data into: silver.erp_loc_a101 <';
INSERT INTO silver.erp_loc_a101 (cid, cntry)

SELECT 
REPLACE (cid, '-', '')cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101;
-- WHERE REPLACE (cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

/*
==================================================================================================
*/


-- Re-run to check the quality of data in the Silver layer
SELECT DISTINCT cntry FROM silver.erp_loc_a101;

SELECT * FROM silver.erp_loc_a101;
