# Data Warehouse and Analytics Project

 This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. 

---
## 🏗️ Data Architecture

The data architecture for this project follows the Medallion Architecture: **Bronze**, **Silver**, and **Gold** layers:

![Data Architecture](https://github.com/yusufm223/sql_data_warehouse_project/blob/main/doc/data_architecture%20new.png?raw=true)

1. **Bronze Layer**  
   Stores raw data as-is from the source systems. Data is ingested from CSV files into a SQL Server database.

2. **Silver Layer**  
   Includes data cleansing, standardization, and normalization processes to prepare data for analysis.

3. **Gold Layer**  
   Contains business-ready data modeled into a star schema, ready for reporting and analytics.

---

## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.
   
---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

   
 

