-- Dimension: Customers

-- After Joining tables, check if any duplicates were introduced by the join logic
SELECT cst_id, COUNT(*) FROM (
	SELECT ci.cst_id, ci.cst_key,ci.cst_firstname,
	ci.cst_lastname,ci.cst_marital_status,ci.cst_gndr,ci.cst_create_date,ca.bdate,ca.gen,la.cntry
	FROM silver.crm_cust_info ci 
	LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
)t GROUP BY cst_id HAVING COUNT(*) >1;

-- Two sources of gender cst_gndr and gen / Which source is the master for thes values? ask an expert
SELECT ci.cst_gndr,ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master for gender info
	ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen -- Data Integration
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
ORDER BY 1,2;

/*
====================================================================
*/

CREATE VIEW gold.dim_customers AS
-- Select the columns to be present in the gold layer
SELECT	
	-- Surrogate Key(Primary Key): System-generated unique identifier assigned to each record in a table
	-- DDL-based generation / Query-based using Window Function (Row_Number). Used to connect data model
	ROW_NUMBER() OVER (ORDER BY cst_id) as customer_key,
	ci.cst_id AS customer_id,		-- rename columns to be user friendly / sort into logical groups to improve readability
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	la.cntry AS country,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date	
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid;

/*
====================================================================
*/


-- Quality Check
SELECT * FROM gold.dim_customers;
SELECT DISTINCT gender FROM gold.dim_customers;
