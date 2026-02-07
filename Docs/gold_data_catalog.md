# Gold Layer Data Catalog

## Overview
The Gold Layer represents the **business-level analytical model** of the data warehouse.  
It is designed using a **star schema** and contains:

- Dimension tables for descriptive attributes.
- Fact tables for transactional metrics.
- Aggregated views for executive KPIs.

All objects in this layer are built from the Silver layer and are optimized for reporting, dashboards, and analytics.

---

## Schema Diagram (Logical)

dim_customers ───────┐  
                     ├── fact_sales  
                     │  
Aggregations derived from fact_sales  

---

## 1. gold.dim_customers
**Purpose:**  
Stores customer master data enriched with geographic attributes.

**Grain:**  
One row per customer.

| Column Name   | Data Type | Description |
|--------------|-----------|-------------|
| customer_key | BIGINT    | Surrogate key generated in the Gold layer. |
| customer_id  | BIGINT    | Business customer identifier from source system. |
| country      | VARCHAR   | Country code of customer's primary address. |
| region       | VARCHAR   | Region or state of customer's address. |

---

## 2. gold.fact_sales
**Purpose:**  
Stores transactional sales data at order-line level.

**Grain:**  
One row per sales document item.

| Column Name            | Data Type | Description |
|------------------------|-----------|-------------|
| order_key              | BIGINT    | Surrogate key for each order line. |
| order_id               | BIGINT    | Sales document identifier. |
| order_item_id          | BIGINT    | Line item number within order. |
| product                | BIGINT    | Product identifier. |
| customer_key           | BIGINT    | Foreign key to dim_customers. |
| customer_id            | BIGINT    | Sold-to customer ID. |
| ship_to_party          | BIGINT    | Shipping customer ID. |
| bill_to_party          | BIGINT    | Billing customer ID. |
| plant                  | VARCHAR   | Fulfillment plant. |
| shipping_point         | VARCHAR   | Shipping location. |
| creation_date          | DATE      | Order creation date. |
| sales_organization     | VARCHAR   | Sales organization. |
| distribution_channel   | VARCHAR   | Distribution channel. |
| sales_group            | VARCHAR   | Sales team. |
| transaction_currency   | VARCHAR   | Currency code. |

---

## 3. gold.agg_top_10_customers_by_sales
**Purpose:**  
Ranks customers by total order volume.

| Column Name      | Data Type | Description |
|------------------|-----------|-------------|
| customer_key     | BIGINT    | Customer surrogate key. |
| number_of_orders | BIGINT    | Total orders placed by customer. |

---

## 4. gold.agg_top_10_countries_by_sales
**Purpose:**  
Ranks countries by sales volume.

| Column Name      | Data Type | Description |
|------------------|-----------|-------------|
| country          | VARCHAR   | Country code. |
| number_of_orders | BIGINT    | Total orders from country. |

---

## 5. gold.agg_orders_by_month
**Purpose:**  
Tracks order trends over time.

| Column Name      | Data Type | Description |
|------------------|-----------|-------------|
| `Month`            | DATE / TEXT | Month bucket (YYYY-MM).       |
| `number_of_orders` | BIGINT      | Orders created in that month. |
