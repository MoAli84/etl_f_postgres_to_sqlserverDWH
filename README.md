# End-to-End ETL Data Warehouse Pipeline

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline that extracts data from a PostgreSQL database, applies data transformations and quality checks using Python, and loads the processed data into a SQL Server Data Warehouse using a star schema design.

## 🔧 Technologies Used
- Python (Pandas, SQLAlchemy)
- PostgreSQL (Source Database)
- SQL Server (Data Warehouse)
- SQLAlchemy & pyodbc

## 🏗️ Data Warehouse Design
The data warehouse follows a **Star Schema** design:

### Dimension Tables
- **dim_customers**
  - customer_id (PK)
  - city
  - state_province
  - country
  - region

- **dim_products**
  - product_id (PK)
  - product_name
  - factory
  - division
  - unit_price

### Fact Table
- **fact_orders**
  - transaction_id (PK)
  - customer_id (FK)
  - product_id (FK)
  - order_id
  - order_date
  - month
  - units
  - total_amount_by_month
  - total_units_by_customer
  - total_orders_by_product

## 🔄 ETL Pipeline Workflow
1. **Extract**
   - Data is extracted from PostgreSQL source tables using SQLAlchemy.

2. **Transform**
   - Remove duplicates
   - Handle missing values
   - Convert date columns
   - Create derived metrics (monthly totals, customer aggregates, product aggregates)

3. **Load**
   - Create tables in SQL Server using DDL statements
   - Load data into dimension tables first
   - Load data into the fact table
   - Enforce primary and foreign key constraints



## 📁 Project Structure
