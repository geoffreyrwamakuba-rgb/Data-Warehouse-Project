
-- CREATE CUSTOMERS TABLE
DROP TABLE IF EXISTS silver.customers;
CREATE TABLE silver.customers (
customer_id BIGINT,
address_id BIGINT,
dwh_creation_date TIMESTAMP DEFAULT NOW(),
PRIMARY KEY (customer_id,address_id));

-- CREATE SALES DOCUMENT TABLE
DROP TABLE IF EXISTS silver.sales_document;
CREATE TABLE silver.sales_document 
(sales_document BIGINT PRIMARY KEY, sales_office VARCHAR(10),
sales_group VARCHAR(10), customer_payment_terms VARCHAR(10),
shipping_condition VARCHAR(10), sales_document_type VARCHAR(10),
sales_organization VARCHAR(10), distribution_channel VARCHAR(10),
organization_division VARCHAR(10), billing_company_code VARCHAR(10),
transaction_currency VARCHAR(10), inco_terms_classification VARCHAR(10),
creation_date DATE, creation_time TIME, 
dwh_creation_date TIMESTAMP DEFAULT NOW());

-- CREATE SALES DOCUMENT ITEM TABLE
DROP TABLE IF EXISTS silver.sales_document_item;
CREATE TABLE silver.sales_document_item
(sales_document BIGINT, sales_document_item BIGINT, 
plant VARCHAR(10), shipping_point VARCHAR(10), sales_document_item_category VARCHAR(10),
product BIGINT, sold_to_party BIGINT, ship_to_party BIGINT,
bill_to_party BIGINT, payer_party BIGINT, inco_terms_classification VARCHAR(10),
dwh_creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (sales_document,sales_document_item));

-- CREATE ADDRESS INFO TABLE
DROP TABLE IF EXISTS silver.address_info;
CREATE TABLE silver.address_info
(address_id BIGINT PRIMARY KEY, address_presentation_code VARCHAR(10), 
country VARCHAR(10), region VARCHAR(10),
dwh_continent VARCHAR(20),
dwh_creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
