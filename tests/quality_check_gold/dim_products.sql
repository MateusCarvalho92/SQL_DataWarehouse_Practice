-- Dimension: Products

-- After Joining tables, check if any duplicates were introduced by the join logic
SELECT prd_key, COUNT(*) FROM (
SELECT pn.prd_id,pn.cat_id,pn.prd_key,pn.prd_nm,pn.prd_cost,pn.prd_line,pn.prd_start_dt,
pc.cat,pc.subcat,pc.maintenance FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL
)t GROUP BY prd_key HAVING COUNT(*) > 1;

-- Check if there are same information twice 


/*
====================================================================
*/

CREATE VIEW gold.dim_products AS
-- rename columns to be user friendly / sort into logical groups to improve readability
SELECT 
-- Surrogate Key(Primary Key): System-generated unique identifier assigned to each record in a table
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date
-- pn.prd_end_dt	-- If End Date is NULL then it is current info of the product
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL; -- Filter out all historical data


/*
====================================================================
*/

-- Quality Check
SELECT * FROM gold.dim_products;
