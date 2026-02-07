/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ==================================================================================================
-- 1. CUSTOMERS TABLE CHECKS
-- ==================================================================================================

-- ==================================================================================================
-- Check for Nulls or duplicates in the candidate Primary Key
-- ==================================================================================================

-- SELECT * FROM bronze.customers LIMIT 100;

SELECT customer_id, count(customer_id) FROM bronze.customers 
GROUP BY customer_id HAVING count(customer_id)>1 OR customer_id is NULL;

SELECT * FROM bronze.customers WHERE customer_id IN 
(SELECT customer_id FROM bronze.customers 
GROUP BY customer_id HAVING count(customer_id)!=1);

SELECT address_id, count(address_id) FROM bronze.customers 
GROUP BY address_id HAVING count(address_id)>1 OR address_id is NULL;

SELECT (customer_id, address_id), count(*) FROM bronze.customers 
GROUP BY customer_id, address_id HAVING count(*)>1 OR address_id is NULL OR customer_id is NULL;

-- ==================================================================================================
-- Conclusion
-- No duplicates in address_id but 2 duplicates in customer_id
-- The same customer has multiple addresses
-- Customer_id cannot be used as a standalone primary key because customers may have multiple addresses.
-- The correct candidate key is the composite (customer_id, address_id)
-- In real ERP: billing address vs shipping address, HQ vs branch, old vs new address
-- ==================================================================================================

-- ==================================================================================================
-- Are primary Keys missing in another table?
-- ==================================================================================================

-- NOTE: NOT IN method is very slow. We have used Anti-Joins below
-- SELECT * FROM bronze.customers
-- WHERE customer_id NOT IN (SELECT sold_to_party FROM bronze.sales_document_item);

-- Missing Keys in the below 100% acceptable and normal - Could be new or inactive customers
SELECT * FROM bronze.customers a LEFT JOIN bronze.sales_document_item b
ON a.customer_id = b.sold_to_party WHERE b.sold_to_party IS NULL;
SELECT * FROM bronze.customers a LEFT JOIN bronze.sales_document_item b
ON a.customer_id = b.ship_to_party WHERE b.ship_to_party IS NULL;
SELECT * FROM bronze.customers a LEFT JOIN bronze.sales_document_item b
ON a.customer_id = b.bill_to_party WHERE b.bill_to_party IS NULL;
SELECT * FROM bronze.customers a LEFT JOIN bronze.sales_document_item b
ON a.customer_id = b.payer_party WHERE b.payer_party IS NULL;

-- foreign key validation - No missing keys
SELECT DISTINCT b.sold_to_party FROM bronze.sales_document_item b
LEFT JOIN bronze.customers a ON a.customer_id = b.sold_to_party WHERE a.customer_id IS NULL;
SELECT DISTINCT b.ship_to_party FROM bronze.sales_document_item b
LEFT JOIN bronze.customers a ON a.customer_id = b.ship_to_party WHERE a.customer_id IS NULL;
SELECT DISTINCT b.bill_to_party FROM bronze.sales_document_item b
LEFT JOIN bronze.customers a ON a.customer_id = b.bill_to_party WHERE a.customer_id IS NULL;
SELECT DISTINCT b.payer_party FROM bronze.sales_document_item b
LEFT JOIN bronze.customers a ON a.customer_id = b.payer_party WHERE a.customer_id IS NULL;


-- No Missing candidate Keys (Addresses for all customers)
SELECT * FROM bronze.customers a LEFT JOIN bronze.address_info b
ON a.address_id = b.address_id WHERE b.address_id IS NULL;


-- ==================================================================================================
-- No NULL values + No unwanted Spaces in BIGINT
-- No Categorisation needed
-- No Dates
-- No Prices & Calculations
-- ==================================================================================================

-- ==================================================================================================
-- 2. SALES DOCUMENT TABLE CHECKS
-- ==================================================================================================

-- No NULLs or duplicates in the candidate Primary Key column.
SELECT sales_document, count(*) FROM bronze.sales_document
GROUP BY sales_document Having count(*)>1 OR sales_document IS NULL;

-- No Primary Key missing in sales_document_item column. There's no line item that cannot be grouped to an order. 
SELECT * FROM bronze.sales_document a LEFT JOIN bronze.sales_document_item b 
ON a.sales_document = b.sales_document WHERE b.sales_document IS NULL;

-- No Unwanted Spaces
SELECT * FROM bronze.sales_document WHERE 
sales_office != TRIM(sales_office)
OR sales_group != TRIM(sales_group)
OR customer_payment_terms != TRIM(customer_payment_terms)
OR shipping_condition != TRIM(shipping_condition)
OR sales_document_type != TRIM(sales_document_type) 
OR sales_organization != TRIM(sales_organization) 
OR distribution_channel != TRIM(distribution_channel) 
OR organization_division != TRIM(organization_division) 
OR billing_company_code != TRIM(billing_company_code)
OR transaction_currency != TRIM(transaction_currency)
OR inco_terms_classification != TRIM(inco_terms_classification);

-- No Null Values
SELECT * FROM bronze.sales_document WHERE 
sales_office IS NULL
OR sales_group IS NULL
OR customer_payment_terms IS NULL
OR shipping_condition IS NULL
OR sales_document_type IS NULL
OR sales_organization IS NULL 
OR distribution_channel IS NULL 
OR organization_division IS NULL 
OR billing_company_code IS NULL
OR transaction_currency IS NULL
OR inco_terms_classification IS NULL;

-- Check currency length is =3
SELECT transaction_currency FROM bronze.sales_document
WHERE length(transaction_currency)!=3;

-- Check No future orders
SELECT * FROM bronze.sales_document 
WHERE creation_date > cast(now() as date);

-- Valid Min & Max date/times
SELECT max(creation_date) FROM bronze.sales_document;
SELECT min(creation_date) FROM bronze.sales_document;
SELECT max(creation_time) FROM bronze.sales_document;
SELECT min(creation_time) FROM bronze.sales_document;

-- ==================================================================================================
-- 3. SALES DOCUMENT ITEM TABLE CHECKS
-- ==================================================================================================

-- The pair of sales_document and sales_document_item acts as a primary key
-- sales_document repeats (multiple items per order)
-- sales_document_item repeats (item 10 exists in many orders)

-- No NULLs
SELECT * FROM bronze.sales_document_item 
WHERE sales_document IS NULL OR sales_document_item IS NULL;
--  Duplicate candidate composite Primary Keys.
SELECT count(*) FROM bronze.sales_document_item
GROUP BY sales_document, sales_document_item Having count(*)>1;

-- No values in sales_document column (sales_document_item table) are missing from sales_document table.
SELECT * FROM bronze.sales_document_item a LEFT JOIN bronze.sales_document b 
ON a.sales_document = b.sales_document WHERE b.sales_document IS NULL;

-- No Unwanted Spaces in string columns
SELECT * FROM bronze.sales_document_item WHERE 
plant != TRIM(plant)
OR shipping_point != TRIM(shipping_point)
OR sales_document_item_category != TRIM(sales_document_item_category) 
OR inco_terms_classification != TRIM(inco_terms_classification);

-- No Null Values
SELECT * FROM bronze.sales_document_item WHERE 
sales_document IS NULL
OR sales_document_item IS NULL
OR plant IS NULL
OR shipping_point IS NULL
OR sales_document_item_category IS NULL
OR product IS NULL 
OR sold_to_party IS NULL 
OR ship_to_party IS NULL 
OR bill_to_party IS NULL 
OR payer_party IS NULL
OR inco_terms_classification IS NULL;


SELECT * FROM bronze.sales_document_item LIMIT 10;

-- ==================================================================================================
-- 4. ADDRESS INFO TABLE CHECKS
-- ==================================================================================================

-- No NULLs or duplicates in the candidate Primary Key column.
SELECT address_id, count(*) FROM bronze.address_info
GROUP BY address_id Having count(*)>1 OR address_id IS NULL;

-- Identified Unused / unlinked addresses - Normal + Acceptable   
SELECT * FROM bronze.address_info a LEFT JOIN bronze.customers b 
ON a.address_id = b.address_id WHERE b.address_id IS NULL;


-- Null Values Check
SELECT count(*) FROM bronze.address_info WHERE 
address_presentation_code IS NULL
OR country IS NULL
OR region IS NULL;

-- Unwanted Spaces Check
SELECT * FROM bronze.address_info WHERE 
region != TRIM(region);
SELECT * FROM bronze.address_info WHERE 
country != TRIM(country);
SELECT * FROM bronze.address_info WHERE 
address_presentation_code != TRIM(address_presentation_code);

-- Visual blanks are not hidden characters
SELECT region, LENGTH(region), OCTET_LENGTH(region)
FROM bronze.address_info
WHERE region IS NOT NULL AND LENGTH(region)>0
ORDER BY OCTET_LENGTH(region);

-- Checking for true empty string ""
SELECT region FROM bronze.address_info
WHERE NULLIF(TRIM(region), '') IS NULL

-- Checking for true empty string ""
SELECT country FROM bronze.address_info
WHERE NULLIF(TRIM(country), '') IS NULL

-- Checking for true empty string ""
SELECT address_presentation_code FROM bronze.address_info
WHERE NULLIF(TRIM(address_presentation_code), '') IS NULL

SELECT * FROM bronze.address_info LIMIT 10;
-- No shortened country over length 2  
SELECT country FROM bronze.address_info WHERE LENGTH(country)>2;

SELECT * FROM bronze.address_info;

SELECT country,
CASE WHEN TRIM(country) = '' THEN NULL
WHEN TRIM(country) IN (
    'DZ','AO','BJ','BW','BF','BI','CV','CM','CF','TD','KM','CD','CG','CI','DJ',
    'EG','GQ','ER','SZ','ET','GA','GM','GH','GN','GW','KE','LS','LR','LY','MG',
    'MW','ML','MR','MU','YT','MA','MZ','NA','NE','NG','RW','RE','ST','SN','SC',
    'SL','SO','ZA','SS','SD','TZ','TG','TN','UG','EH','ZM','ZW'
  ) THEN 'Africa'

  WHEN TRIM(country) IN (
    'AL','AD','AT','BY','BE','BA','BG','HR','CY','CZ','DK','EE','FO','FI','FR',
    'DE','GI','GR','GL','GG','VA','HU','IS','IE','IM','IT','JE','LV','LI','LT',
    'LU','MT','MD','MC','ME','NL','MK','NO','PL','PT','RO','RU','SM','RS','SK',
    'SI','ES','SJ','SE','CH','UA','GB','AX'
  ) THEN 'Europe'

  WHEN TRIM(country) IN (
    'AF','AM','AZ','BH','BD','BT','BN','KH','CN','GE','HK','IN','ID','IR','IQ',
    'IL','JP','JO','KZ','KP','KR','KW','KG','LA','LB','MO','MY','MV','MN','MM',
    'NP','OM','PK','PS','PH','QA','SA','SG','LK','SY','TW','TJ','TH','TL','TR',
    'TM','AE','UZ','VN','YE'
  ) THEN 'Asia'

  WHEN TRIM(country) IN (
    'AG','AI','AW','BS','BB','BZ','BM','BQ','VG','CA','KY','CR','CU','CW','DM',
    'DO','SV','GL','GD','GP','GT','HT','HN','JM','MQ','MS','MX','NI','PA','PR',
    'BL','KN','LC','MF','PM','VC','SX','TT','TC','US','UM','VI'
  ) THEN 'North America'

  WHEN TRIM(country) IN (
    'AR','BO','BR','CL','CO','EC','FK','GF','GY','PY','PE','SR','UY','VE'
  ) THEN 'South America'

  WHEN TRIM(country) IN (
    'AS','AU','CK','CX','CC','FJ','PF','GU','KI','MH','FM','NR','NC','NZ','NU',
    'NF','MP','PW','PG','PN','WS','SB','TK','TO','TV','VU','WF'
  ) THEN 'Oceania'

  WHEN TRIM(country) IN (
    'AQ','BV','TF','HM','GS'
  ) THEN 'Antarctica'

  ELSE 'Unknown'
END AS dwh_continent
FROM bronze.address_info;
