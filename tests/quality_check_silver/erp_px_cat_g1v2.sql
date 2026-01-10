-- To check if info match to join the tables
SELECT cat_id FROM silver.crm_prd_info;

-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance) OR subcat != TRIM(subcat) OR cat != TRIM(cat);

-- Quality Check: Check the consistency of values in low cardinality columns
-- Data Standardization & Consistency
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2; -- check all columns

/*
==================================================================================================
*/


PRINT '> Truncating Table: silver.erp_px_cat_g1v2 <';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '> Inserting Data into: silver.erp_px_cat_g1v2 <';
INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)

SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;
-- WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info)


/*
==================================================================================================
*/


-- Re-run to check the quality of data in the Silver layer
SELECT * FROM silver.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance) OR subcat != TRIM(subcat) OR cat != TRIM(cat);


SELECT DISTINCT subcat FROM silver.erp_px_cat_g1v2; -- check all columns

SELECT * FROM silver.erp_px_cat_g1v2;
