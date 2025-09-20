# Databricks-EndToEnd-Project

# Retail Data Ingestion and Processing using Databricks

## Project Overview
This project demonstrates an **end-to-end retail data pipeline** built on **Databricks**, following the **Medallion Architecture (Bronze → Silver → Gold)**.  
It handles incremental data ingestion, transformation, and aggregation while maintaining **high data quality and performance**.

---

## Features
- **End-to-end ETL pipeline** using Databricks notebooks and job pipelines.  
- **Incremental data ingestion** using Databricks Autoloader into the Bronze layer on Azure ADLS.  
- **Silver staging tables** and **Gold production-ready dimension tables** for curated data.  
- **Fact tables** for aggregated metrics and reporting.  
- **Delta Lake** for ACID-compliant storage and **Unity Catalog** for governance.  
- **Slowly Changing Dimensions (SCD) Type 1 & Type 2** implemented for dimension tables.  
- **Reusable ETL workflows** via Databricks jobs and pipelines.  
- Optimized for **performance and scalability** to handle large volumes of retail data.  

---

## Architecture
The project follows the **Medallion Architecture**:  

1. **Bronze Layer** – Raw data ingestion using Delta Lake.  
2. **Silver Layer** – Cleaned and transformed staging tables.  
3. **Gold Layer** – Curated, production-ready tables and fact tables for reporting and analytics.  

## Technologies Used
- **Databricks**  
- **Azure Data Lake Storage (ADLS)**  
- **Delta Lake**  
- **Python & PySpark**  
- **Databricks Jobs & Pipelines**  
- **Unity Catalog for Governance**  

---

## Key Highlights
- Automated ETL pipeline with **incremental data load**.  
- Efficient handling of large datasets using **Delta Lake and cluster orchestration**.  
- **SCD Type 1 and Type 2** implemented for dimension table history tracking.  
- Reusable and maintainable pipelines for **production-ready retail analytics**.  

