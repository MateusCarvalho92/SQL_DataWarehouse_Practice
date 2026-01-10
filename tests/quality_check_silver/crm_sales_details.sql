-- Check for Invalid Dates
-- Negative numbers or zeros can't be cast to a date
-- In this scenario, the length of the date must be 8
-- Check sls_order_dt, sls_order_dt and sls_due_dt
SELECT NULLIF(sls_order_dt,0) FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20300101  -- todays date
OR sls_order_dt < 19900101; -- Date when company started

-- Check for Invalid Date Orders
SELECT * FROM bronze.crm_sales_details -- End date must not be earlier than the start date
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Business Rules: Check Data Consistenct:
-- Sales = Quantity * Price
-- Negatives, Zeros, NULLs are not allowed
SELECT sls_sales, sls_quantity, sls_price FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
/* REMINDER: before doing any Transformations, talk to an expert from business or source system
   Solution 1: Data issues will be fixed directly in source system
   Solution 2: Data issues has to be fixed in DWH with the expert help
        Rules: If Sales is negative, zero or null, derive it using Quantity and Price
               If Price is zero or null, calculate it using Sales and Quantity
               If Price is negative, convert it to a positive value
*/
-- Solution 2:
SELECT sls_sales, sls_quantity, sls_price,
-- Rule 1
CASE WHEN sls_sales IS NULL OR sls_sales <0 OR sls_sales != sls_quantity * ABS(sls_price) -- Returns absolute value of a number
     THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END sls_sales,
-- Rule 2 and 3
CASE WHEN sls_price IS NULL OR sls_price <= 0
     THEN sls_sales / NULLIF(sls_quantity, 0)
     ELSE sls_price
END sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

/*
==================================================================================================
*/

PRINT '> Truncating Table: silver.crm_sales_details <';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '> Inserting Data into: silver.crm_sales_details <';
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE) -- CAST INT to VARCHAR then VARCHAR to DATE
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE) -- CAST INT to VARCHAR then VARCHAR to DATE
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE) -- CAST INT to VARCHAR then VARCHAR to DATE
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <0 OR sls_sales != sls_quantity * ABS(sls_price) -- Returns absolute value of a number
     THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
     THEN sls_sales / NULLIF(sls_quantity, 0)
     ELSE sls_price
END sls_price
FROM bronze.crm_sales_details;
-- WHERE sls_ord_num != TRIM(sls_ord_num) -- Check issues with unwanted spaces
-- WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info) -- Check columns integrity for JOIN
-- WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info) -- No Issues = good

/*
==================================================================================================
*/


-- Re-run to check the quality of data in the Silver layer
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT sls_sales, sls_quantity, sls_price FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

SELECT * FROM silver.crm_sales_details;
