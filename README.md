# Data Warehouse Project – Medallion Architecture
## Overview

### This project implements a modern data warehouse using the Bronze → Silver → Gold layered approach.

The goal is to:
- Ingest raw ERP-style data
- Clean and validate it
- Transform it into business-ready analytical models (Star Schema)
- The final Gold layer provides dimension and fact views for reporting and analytics.

## Architecture
| **Layers** | **Layer Purpose**                                                                                                         |
|------------|---------------------------------------------------------------------------------------------------------------------------|
| Bronze     |  Raw ingestion layer. Data is loaded exactly as received from source systems. No transformations.                         |
| Silver     |  Cleansed and standardized layer. Data quality checks, type cleaning, trimming, deduplication, and business rules applied.|
| Gold       |  Business layer. Star schema (dimensions + facts) and aggregations for analytics.                                         | 

## Bronze Layer - Store raw source data exactly as ingested.
### Tables:
- bronze.customers
- bronze.sales_document
- bronze.sales_document_item
- bronze.address_info

**Stored procedure - bronze.load_bronze()**

**No constraints or business logic are applied in Bronze.**

## Silver Layer - Cleans and validates data before it becomes analytical.
### Key Features
- Trimming whitespace
- Converting empty strings to NULL
- Removing future dates
- Foreign key validation
- Data quality checks
- Composite keys where required
### Tables:
- silver.customers
- silver.sales_document
- silver.sales_document_item
- silver.address_info

**Stored procedure - silver.load_silver()**

**Quality checks - silver_layer_quality_checks.sql**
  
## Gold Layer (Star Schema) - Business-ready analytical layer.
### Views
- gold.dim_customers
- gold.fact_sales
- gold.agg_top_10_customers_by_sales
- gold.agg_top_10_countries_by_sales
- gold.agg_orders_by_month

**This project follows a Kimball-style dimensional model:**
- Facts at the lowest grain (order line)
- Surrogate keys for dimensions
- Star schema design
- Aggregations built on top of facts
- Gold Data Catalog

**Full column-level documentation is available in Docs/gold_data_catalog.md**
- Column names
- Data types
- Business descriptions

**Diagrams - All architectural diagrams are provided in /Docs:**

| **File**                 | **Description**                   |
|--------------------------|-----------------------------------|
| Data_Flow.drawio         | End-to-end pipeline flow          |
| Data_Integration.drawio  | Layer interactions                |
| Data_Model.drawio	       | Star schema model                 |

