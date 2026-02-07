/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

DROP VIEW IF EXISTS gold.dim_customers CASCADE;  
DROP VIEW IF EXISTS gold.fact_sales CASCADE;
DROP VIEW IF EXISTS gold.agg_top_10_customers_by_sales CASCADE;
DROP VIEW IF EXISTS gold.agg_top_10_countries_by_sales CASCADE;
DROP VIEW IF EXISTS gold.agg_orders_by_month CASCADE;
-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
CREATE VIEW gold.dim_customers AS 
SELECT 
ROW_NUMBER() OVER(ORDER BY a.customer_id) AS customer_key, -- surrogate key
a.customer_id AS customer_id,
b.country AS country,
b.region AS region
FROM silver.customers a LEFT JOIN silver.address_info b ON a.address_id = b.address_id;

SELECT * FROM gold.dim_customers;

-- =============================================================================
-- Create Facts: gold.fact_sales
-- =============================================================================
CREATE VIEW gold.fact_sales AS
SELECT
ROW_NUMBER() OVER(ORDER BY s.sales_document) AS order_key, -- surrogate key
	s.sales_document AS order_id,
	s.sales_document_item AS order_item_id,
	s.product,
	cu.customer_key AS customer_key, 
	s.sold_to_party AS customer_id,
	s.ship_to_party,
	s.bill_to_party,
	s.plant,
	s.shipping_point,
	sd.creation_date,
	sd.sales_organization,
	sd.distribution_channel,
	sd.sales_group,
	sd.transaction_currency
FROM silver.sales_document_item s
LEFT JOIN gold.dim_customers cu ON s.sold_to_party = cu.customer_id
JOIN silver.sales_document sd ON s.sales_document = sd.sales_document;

-- =============================================================================
-- Create Aggregation: gold.agg_top_10_customers_by_sales
-- =============================================================================
CREATE VIEW gold.agg_top_10_customers_by_sales AS
SELECT a.customer_key, count(b.order_id) AS number_of_orders FROM gold.dim_customers a
LEFT JOIN gold.fact_sales b ON a.customer_key = b.customer_key
GROUP BY a.customer_key ORDER BY count(b.order_id) DESC LIMIT 10;

SELECT * FROM gold.agg_top_10_customers_by_sales;

-- =============================================================================
-- Create Aggregation: gold.agg_top_10_countries_by_sales
-- =============================================================================
SELECT * FROM gold.fact_sales LIMIT 5;
SELECT * FROM gold.dim_customers LIMIT 5;

CREATE VIEW gold.agg_top_10_countries_by_sales AS
SELECT b.country, count(a.order_id) AS number_of_orders 
FROM gold.fact_sales a
LEFT JOIN gold.dim_customers b ON a.customer_id = b.customer_id
GROUP BY b.country ORDER BY count(a.order_id) DESC LIMIT 10;

SELECT * FROM gold.agg_top_10_countries_by_sales;

-- =============================================================================
-- Create Aggregation: gold.agg_orders_by_month
-- =============================================================================
CREATE VIEW gold.agg_orders_by_month AS
WITH CTE1 AS (

SELECT a.creation_date, 
TO_CHAR(DATE_TRUNC('month',a.creation_date),'yyyy-mm') AS "Month",
a.order_id
FROM gold.fact_sales a)

SELECT "Month", 
count(order_id) AS number_of_orders
FROM CTE1 GROUP BY "Month" ORDER BY "Month" DESC;

SELECT * FROM gold.agg_orders_by_month ORDER BY number_of_orders DESC;