-- Change-Over-Time (Trends)
-- Analyze how a measure evolves over time
-- Helps track trends and identify seasonality in your data
-- Total Sales by year / Average cost by month, etc

/* Analyse sales performance over time */
SELECT
YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date)


-- Cumulative Analysis
-- Aggregate the data progressively over time
-- Helps to understtatnd whether our business is growing or declining

/* Calculate the total sales per month and the runing total of sales over time */
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total,
AVG(avg_price)OVER (ORDER BY order_date) AS moving_average_price
FROM 
(
SELECT
DATETRUNC(month, order_date)AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date)
)t


-- Performance analysis
-- Comparing the current value to a target value
-- Helps measure success and compare performance

/* Analyze the yearly performance of products by comparing each product's sales to 
   both it's average sales performance and the previous year's sales */
WITH yearly_product_sales AS (
	SELECT
	YEAR(f.order_date) AS order_year,
	p.product_name,
	SUM(f.sales_amount) AS current_sales
	FROM fact_sales f
	LEFT JOIN dim_products p ON f.product_key = p.product_key
	WHERE f.order_date IS NOT NULL
	GROUP BY YEAR(f.order_date),p.product_name
)
SELECT
order_year,
product_name,
current_sales,
-- average sales performance
AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
     WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
	 ELSE 'Avg'
END avg_change,
-- previous year comparsion / year over year analysis
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_previous_year,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
     WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
	 ELSE 'No Change'
END previous_year_change
FROM yearly_product_sales
ORDER BY product_name,order_year


-- Part-to-Whole
-- Analyze how an individual part is performing compared to the overall,
-- allowing us to understand which category has the greatest impact on the business

/* Which categories contribute the most to overall sales */
WITH category_sales AS (
SELECT 
category,
SUM(sales_amount) AS total_sales
FROM fact_sales f
LEFT JOIN dim_products p ON p.product_key = f.product_key
GROUP BY category )

SELECT
category,
total_sales,
SUM(total_sales)OVER() AS overall_sales,
CONCAT(ROUND((CAST (total_sales AS FLOAT) / SUM(total_sales) OVER ())*100, 2), '%') AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC


-- Data Segmentation
-- Group the data based on a specific range.
-- Helps understand the correlation between two measures

/* Segment products into cost ranges and count how many products fall into each segment */
WITH product_segments AS (
SELECT 
product_key,
product_name,
cost,
-- segment products into cost range
CASE WHEN cost < 100 THEN 'Below 100'
     WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	 WHEN cost BETWEEN 500 and 1000 THEN '500-1000'
	 ELSE 'Above 1000'
END cost_range
FROM dim_products )

SELECT
cost_range,
COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC

/* Group customers into three segments based on their spending behaviour:
	- VIP: Customers with at least 12 months of history and spending more than $5,000.
	- Regular: Customers with at least 12 months of history but spend $5,000 or less.
	- New: Customers with a lifespan less than 12 months
And find the total number of customers by each group */
WITH customer_spending AS (
	SELECT
	c.customer_key,
	SUM(f.sales_amount) AS total_spending,
	MIN(order_date) AS first_order,
	MAX(order_date) AS last_order,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
	FROM fact_sales f
	LEFT JOIN dim_customers c ON f.customer_key = c.customer_key
	GROUP BY c.customer_key
)
SELECT
customer_segment,
COUNT(customer_key) AS total_customers
FROM (
	SELECT
	customer_key,
	total_spending,
	lifespan,
	CASE WHEN lifespan >= 12 and total_spending > 5000 THEN 'VIP'
		 WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
		 ELSE 'NEW'
	END customer_segment
	FROM customer_spending ) t
GROUP BY customer_segment
ORDER BY total_customers DESC;


/*
================================================================================
	Customer Report
================================================================================
Purpose:
	- This report consolidates key customer metrics and behaviours

Highlights:
	1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates customer-level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last order)
		- average order value
		- average monthly spend
================================================================================
*/
/* -----------------------------------------------------------------------
1) Base Query: Retrives core columns from tables
------------------------------------------------------------------------*/
CREATE VIEW report_customers AS
WITH base_query AS (
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name,' ',c.last_name) AS customer_name,
DATEDIFF(year,c.birthdate,GETDATE()) age
FROM fact_sales f
LEFT JOIN dim_customers c ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL )
/* -----------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
------------------------------------------------------------------------*/
, customer_aggregation AS(
SELECT
customer_key,
customer_number,
customer_name,
age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) AS last_order_date,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM base_query
GROUP BY 
	customer_key,
	customer_number,
	customer_name,
	age )

/* -----------------------------------------------------------------------
3 and 4) Customer Segmentation and KPIs
------------------------------------------------------------------------*/
SELECT
customer_key,
customer_number,
customer_name,
age,
CASE WHEN age < 20 THEN 'Under 20'
	 WHEN age between 20 and 29 THEN '20-29'
	 WHEN age between 30 and 39 THEN '30-39'
	 WHEN age between 40 and 49 THEN '40-49'
	 ELSE 'Above 50'
END AS age_group,
CASE WHEN lifespan >= 12 and total_sales > 5000 THEN 'VIP'
	 WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
	 ELSE 'NEW'
END AS customer_segment,
total_orders,
total_sales,
total_quantity,
total_products,
last_order_date,
DATEDIFF(month, last_order_date, GETDATE()) AS recency,
lifespan,
-- Compute average order value (AVO)
CASE WHEN total_orders = 0 THEN 0
	 ELSE total_sales / total_orders
END AS avg_order_value,
-- Compute average monthly spend
CASE WHEN lifespan = 0 THEN total_sales
	 ELSE total_sales / lifespan
END avg_monthly_spend
FROM customer_aggregation
