# Sales-data-warehouse

This project demonstrates the implementation of a **modern data warehouse** using the **Medallion Architecture** — structured into **Bronze**, **Silver**, and **Gold** layers — to transform raw sales data into business-ready insights.

## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

## 🛠️ Technologies Used

- **Database**: SQL Server  
- **ETL Scripting**: T-SQL (Stored Procedures, Views)  
- **Data Source**: CSV files  
- **Data Modeling**: Star Schema design 
