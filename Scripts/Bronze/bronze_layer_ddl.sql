-- CREATE CUSTOMERS TABLE
DROP TABLE IF EXISTS bronze.customers;
CREATE TABLE bronze.customers (customer_id BIGINT, address_id BIGINT);

-- CREATE SALES DOCUMENT TABLE
DROP TABLE IF EXISTS bronze.sales_document;
CREATE TABLE bronze.sales_document 
(sales_document BIGINT, sales_office VARCHAR(10),
sales_group VARCHAR(10), customer_payment_terms VARCHAR(10),
shipping_condition VARCHAR(10), sales_document_type VARCHAR(10),
sales_organization VARCHAR(10), distribution_channel VARCHAR(10),
organization_division VARCHAR(10), billing_company_code VARCHAR(10),
transaction_currency VARCHAR(10), inco_terms_classification VARCHAR(10),
creation_date DATE, creation_time TIME);

-- CREATE SALES DOCUMENT ITEM TABLE
DROP TABLE IF EXISTS bronze.sales_document_item;
CREATE TABLE bronze.sales_document_item
(sales_document BIGINT, sales_document_item BIGINT, 
plant VARCHAR(10) , shipping_point VARCHAR(10) , sales_document_item_category VARCHAR(10),
product BIGINT, sold_to_party BIGINT, ship_to_party BIGINT,
bill_to_party BIGINT, payer_party BIGINT, inco_terms_classification VARCHAR(10));

-- CREATE ADDRESS INFO TABLE
DROP TABLE IF EXISTS bronze.address_info;
CREATE TABLE bronze.address_info
(address_id BIGINT, address_presentation_code VARCHAR(10), 
country VARCHAR(10), region VARCHAR(10));
