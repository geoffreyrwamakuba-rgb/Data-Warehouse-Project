/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external parquet files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the INSERT INTO ... SELECT command to load data from raw python ingested tables to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze() 
LANGUAGE plpgsql
AS $$
DECLARE start_time TIMESTAMP; 
		end_time TIMESTAMP;
		batch_start_time TIMESTAMP;
		batch_end_time TIMESTAMP;
BEGIN

RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING BRONZE LAYER';
RAISE NOTICE '=================================================';

RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING CUSTOMERS TABLE';
RAISE NOTICE '=================================================';

batch_start_time := NOW();
start_time := NOW();

TRUNCATE TABLE bronze.customers; 
INSERT INTO bronze.customers (customer_id, address_id)
(SELECT CAST("CUSTOMER" AS BIGINT), CAST("ADDRESSID" AS BIGINT) FROM customers);

end_time := NOW();
RAISE NOTICE '>> Customers loading time = % ms', ROUND(EXTRACT(EPOCH FROM age(end_time,start_time))*1000);
RAISE NOTICE '-----------';

RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING SALES DOCUMENT TABLE';
RAISE NOTICE '=================================================';

start_time := NOW();
RAISE NOTICE '>> Truncating bronze.sales_document';
TRUNCATE TABLE bronze.sales_document; 

RAISE NOTICE '>> Inserting into bronze.sales_document';
INSERT INTO bronze.sales_document (sales_document, sales_office,
sales_group, customer_payment_terms,
shipping_condition, sales_document_type,
sales_organization, distribution_channel,
organization_division, billing_company_code,
transaction_currency, inco_terms_classification,
creation_date, creation_time)
(SELECT 
CAST("SALESDOCUMENT" AS BIGINT), CAST("SALESOFFICE" AS VARCHAR(10)),
CAST("SALESGROUP" AS VARCHAR(10)), CAST("CUSTOMERPAYMENTTERMS" AS VARCHAR(10)),
CAST("SHIPPINGCONDITION" AS VARCHAR(10)), CAST("SALESDOCUMENTTYPE" AS VARCHAR(10)),
CAST("SALESORGANIZATION" AS VARCHAR(10)), CAST("DISTRIBUTIONCHANNEL" AS VARCHAR(10)),
CAST("ORGANIZATIONDIVISION" AS VARCHAR(10)), CAST("BILLINGCOMPANYCODE" AS VARCHAR(10)),
CAST("TRANSACTIONCURRENCY" AS VARCHAR(10)), CAST("INCOTERMSCLASSIFICATION" AS VARCHAR(10)),
CAST("CREATIONDATE" AS DATE), CAST("CREATIONTIME" AS TIME) FROM "SalesDocument_test")
UNION ALL
(SELECT 
CAST("SALESDOCUMENT" AS BIGINT), CAST("SALESOFFICE" AS VARCHAR(10)),
CAST("SALESGROUP" AS VARCHAR(10)), CAST("CUSTOMERPAYMENTTERMS" AS VARCHAR(10)),
CAST("SHIPPINGCONDITION" AS VARCHAR(10)), CAST("SALESDOCUMENTTYPE" AS VARCHAR(10)),
CAST("SALESORGANIZATION" AS VARCHAR(10)), CAST("DISTRIBUTIONCHANNEL" AS VARCHAR(10)),
CAST("ORGANIZATIONDIVISION" AS VARCHAR(10)), CAST("BILLINGCOMPANYCODE" AS VARCHAR(10)),
CAST("TRANSACTIONCURRENCY" AS VARCHAR(10)), CAST("INCOTERMSCLASSIFICATION" AS VARCHAR(10)),
CAST("CREATIONDATE" AS DATE), CAST("CREATIONTIME" AS TIME) FROM "SalesDocument_train");

end_time := NOW();
RAISE NOTICE '>> Sales document loading time = % ms', ROUND(EXTRACT(EPOCH FROM age(end_time,start_time))*1000);
RAISE NOTICE '-----------';


RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING SALES DOCUMENT ITEM TABLE';
RAISE NOTICE '=================================================';

start_time := NOW();
RAISE NOTICE '>> Truncating bronze.sales_document_item';
TRUNCATE TABLE bronze.sales_document_item;

RAISE NOTICE '>> Inserting into bronze.sales_document_item';
INSERT INTO bronze.sales_document_item
(Sales_document, Sales_document_item, 
Plant, Shipping_point, Sales_document_item_category,
Product, Sold_to_party, Ship_to_party,
Bill_to_party, Payer_party, Inco_terms_classification)
(SELECT 
CAST("SALESDOCUMENT" AS BIGINT), CAST("SALESDOCUMENTITEM" AS BIGINT),
CAST("PLANT" AS VARCHAR(10)), CAST("SHIPPINGPOINT" AS VARCHAR(10)),
CAST("SALESDOCUMENTITEMCATEGORY" AS VARCHAR(10)), CAST("PRODUCT" AS BIGINT),
CAST("SOLDTOPARTY" AS BIGINT), CAST("SHIPTOPARTY" AS BIGINT),
CAST("BILLTOPARTY" AS BIGINT), CAST("PAYERPARTY" AS BIGINT),
CAST("INCOTERMSCLASSIFICATION" AS VARCHAR(10)) FROM "SalesDocumentItem_test")
UNION ALL
(SELECT 
CAST("SALESDOCUMENT" AS BIGINT), CAST("SALESDOCUMENTITEM" AS BIGINT),
CAST("PLANT" AS VARCHAR(10)), CAST("SHIPPINGPOINT" AS VARCHAR(10)),
CAST("SALESDOCUMENTITEMCATEGORY" AS VARCHAR(10)), CAST("PRODUCT" AS BIGINT),
CAST("SOLDTOPARTY" AS BIGINT), CAST("SHIPTOPARTY" AS BIGINT),
CAST("BILLTOPARTY" AS BIGINT), CAST("PAYERPARTY" AS BIGINT),
CAST("INCOTERMSCLASSIFICATION" AS VARCHAR(10)) FROM "SalesDocumentItem_train");

end_time := NOW();
RAISE NOTICE '>> Sales document item loading time = % ms', ROUND(EXTRACT(EPOCH FROM age(end_time,start_time))*1000);
RAISE NOTICE '-----------';

RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING ADDRESS TABLE';
RAISE NOTICE '=================================================';

start_time := NOW();
RAISE NOTICE '>> Truncating bronze.address_info';
TRUNCATE TABLE bronze.address_info;

RAISE NOTICE '>> Inserting into bronze.address_info';
INSERT INTO bronze.address_info
(Address_id, Address_presentation_code, 
Country, Region)
SELECT CAST("ADDRESSID" AS BIGINT), CAST("ADDRESSREPRESENTATIONCODE" AS VARCHAR(10)), 
CAST("COUNTRY" AS VARCHAR(10)), CAST("REGION" AS VARCHAR(10)) FROM "AddrOrgNamePostalAddress";

end_time := NOW();
RAISE NOTICE '>> Address loading time = % ms', ROUND(EXTRACT(EPOCH FROM age(end_time,start_time))*1000);
RAISE NOTICE '-----------';

batch_end_time := NOW();
RAISE NOTICE '>> Total loading time = % ms', ROUND(EXTRACT(EPOCH FROM age(batch_end_time,batch_start_time))*1000);
RAISE NOTICE '-----------';
 
EXCEPTION 
	WHEN others THEN
	
RAISE NOTICE '=================================================';
RAISE NOTICE 'ERROR OCCURRED WHILE LOADING THE BRONZE LAYER';
RAISE NOTICE 'ERROR MESSAGE: %', SQLERRM;
RAISE NOTICE 'ERROR STATE: %', SQLSTATE;
RAISE NOTICE '=================================================';

END; $$; 
