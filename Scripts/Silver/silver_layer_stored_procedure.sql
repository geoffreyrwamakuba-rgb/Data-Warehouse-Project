/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates silver tables.
		- Inserts transformed and cleansed data from silver into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver() 
LANGUAGE plpgsql
AS $$
DECLARE start_time TIMESTAMP; 
		end_time TIMESTAMP;
		batch_start_time TIMESTAMP;
		batch_end_time TIMESTAMP;
BEGIN

RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING SILVER LAYER';
RAISE NOTICE '=================================================';

RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING CUSTOMERS TABLE';
RAISE NOTICE '=================================================';

batch_start_time := NOW();
start_time := NOW();

TRUNCATE TABLE silver.customers; 
INSERT INTO silver.customers (customer_id, address_id)
-- Protect against future address_id duplicates with distinct
(SELECT DISTINCT customer_id, address_id FROM bronze.customers);

end_time := NOW();
RAISE NOTICE '>> Customers loading time = % ms', ROUND(EXTRACT(EPOCH FROM age(end_time,start_time))*1000);
RAISE NOTICE '-----------';

RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING SALES DOCUMENT TABLE';
RAISE NOTICE '=================================================';

start_time := NOW();
RAISE NOTICE '>> Truncating silver.sales_document';
TRUNCATE TABLE silver.sales_document; 

RAISE NOTICE '>> Inserting into silver.sales_document';
INSERT INTO silver.sales_document 
(sales_document, sales_office,
sales_group, customer_payment_terms,
shipping_condition, sales_document_type,
sales_organization, distribution_channel,
organization_division, billing_company_code,
transaction_currency, inco_terms_classification,
creation_date, creation_time)
SELECT 
sales_document, sales_office,
sales_group, customer_payment_terms,
shipping_condition, sales_document_type,
sales_organization, distribution_channel,
organization_division, billing_company_code,
transaction_currency, inco_terms_classification,
(CASE WHEN creation_date > CAST(NOW() AS DATE) THEN NULL ELSE creation_date 
END) AS creation_date -- Set future dates to NULL
, creation_time FROM bronze.sales_document;

end_time := NOW();

RAISE NOTICE '>> Sales document loading time = % ms', ROUND(EXTRACT(EPOCH FROM age(end_time,start_time))*1000);
RAISE NOTICE '-----------';


RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING SALES DOCUMENT ITEM TABLE';
RAISE NOTICE '=================================================';

start_time := NOW();
RAISE NOTICE '>> Truncating silver.sales_document_item';
TRUNCATE TABLE silver.sales_document_item;

RAISE NOTICE '>> Inserting into silver.sales_document_item';
INSERT INTO silver.sales_document_item
(Sales_document, Sales_document_item, 
Plant, Shipping_point, Sales_document_item_category,
Product, Sold_to_party, Ship_to_party,
Bill_to_party, Payer_party, Inco_terms_classification)
SELECT 
Sales_document, Sales_document_item, 
Plant, Shipping_point, Sales_document_item_category,
Product, Sold_to_party, Ship_to_party,
Bill_to_party, Payer_party, Inco_terms_classification
FROM bronze.sales_document_item;

end_time := NOW();
RAISE NOTICE '>> Sales document item loading time = % ms', ROUND(EXTRACT(EPOCH FROM age(end_time,start_time))*1000);
RAISE NOTICE '-----------';

RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING ADDRESS TABLE';
RAISE NOTICE '=================================================';

start_time := NOW();
RAISE NOTICE '>> Truncating silver.address_info';
TRUNCATE silver.address_info;

RAISE NOTICE '>> Inserting into silver.address_info';
INSERT INTO silver.address_info
(Address_id, address_presentation_code, 
country, region, dwh_continent)
SELECT Address_id, 
NULLIF(TRIM(address_presentation_code), '') AS address_presentation_code,
NULLIF(TRIM(country), '') AS country,
NULLIF(TRIM(region),'') AS region,
(CASE WHEN TRIM(country) = '' THEN NULL
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
END) AS dwh_continent
FROM bronze.address_info;

end_time := NOW();
RAISE NOTICE '>> Address loading time = % ms', ROUND(EXTRACT(EPOCH FROM age(end_time,start_time))*1000);
RAISE NOTICE '-----------';

batch_end_time := NOW();
RAISE NOTICE '>> Total loading time = % ms', ROUND(EXTRACT(EPOCH FROM age(batch_end_time,batch_start_time))*1000);
RAISE NOTICE '-----------';
 
EXCEPTION WHEN OTHERS THEN
RAISE NOTICE '=================================================';
RAISE NOTICE 'ERROR OCCURED WHILE LOADING THE SILVER LAYER';
RAISE NOTICE 'ERROR MESSAGE: %', SQLERRM;
RAISE NOTICE 'ERROR STATE: %', SQLSTATE;
RAISE NOTICE '=================================================';
RAISE;   -- rethrow the error so the transaction fails
END; $$;
