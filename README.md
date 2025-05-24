# 📦 DataStream Medallion Warehouse

This project implements a Medallion Architecture-based DataStream Medallion Warehouse using MySQL and SSMS. It focuses on data ingestion, cleansing, transformation, and modeling to support advanced analytics and business intelligence. The primary data sources are **CRM** and **ERP** systems, allowing for the seamless integration of customer and business operations data. This architecture enhances data quality, scalability, and efficiency in data analysis and reporting by organizing data into different layers.


## 📖 Project Overview

This project involves:

- **Data Architecture**: Designing a Modern DataStream Medallion Warehouse using the Medallion Architecture with Bronze, Silver, and Gold layers.
- **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
- **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
- **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

🎯 This repository is an excellent resource for professionals and students looking to showcase expertise in:

- SQL Development
- Data Architecture
- Data Engineering
- ETL Pipeline Development
- Data Modeling
- Data Analytics



## 💡 Objectives
- Establish a scalable, modular data warehouse  
- Enable business-ready analytics  
- Improve data quality and consistency  
- Support BI dashboards and reporting  


## 🧱 Architecture Overview
This project is based on the Medallion Architecture, consisting of three layers:

- **Bronze Layer**: Handles raw data ingestion from CRM, ERP, and external systems.
- **Silver Layer**: Processes and transforms raw data, applying business rules and validations. 
- **Gold Layer**: Aggregates and models data into fact and dimension tables for reporting.
 

**⚙️ ETL/ELT Pipeline Description**

Data flows from raw ingestion (Bronze), through cleaning/transformation (Silver), to a final modeled format (Gold). Stored procedures, scripts, and views are used for automation.




## 🛠️ Technologies Used

- **MySQL Workbench** – SQL-based ETL logic and transformations  
- **SSMS (SQL Server Management Studio)** – SQL-based ETL logic and transformations 
- **GitHub** – Version control and collaboration  
- **draw.io**: Used for creating architectural diagrams.



### 📊 Data Modeling
The project uses a Star Schema model:

- **Fact Table**: Sales
- **Dimension Tables**: Customer, Product




## 📦 GitHub Repository
This project is hosted on GitHub. You can find the repository at:  
[DataStream Medallion Warehouse](https://github.com/MOHAMED-EL-HADDIOUI/DataStream-Medallion-Warehouse.git)




## 🚀 Running the Project
To run the DataStream Medallion Warehouse project, follow these steps:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/MOHAMED-EL-HADDIOUI/DataStream-Medallion-Warehouse.git
   cd DataStream-Medallion-Warehouse
   ```

2. **Set Up the Database**:
   - Open your MySQL Workbench or SSMS.
   - Run the `sql_scripts/initialize_database_and_schemas.sql` script to create the database and schemas.

3. **Load Data into the Bronze Layer**:
   - Run the `sql_scripts_mysql/bronze_layer/data_load_bronze.sql` script to load raw data into the Bronze Layer.

4. **Transform and Load Data into the Silver Layer**:
   - Run the `sql_scripts_mysql/silver_layer/data_load_silver.sql` script to transform and load data into the Silver Layer.

5. **Generate Business-Ready Datasets in the Gold Layer**:
   - Run the `sql_scripts_mysql/gold_layer/gold_layer_development.sql` script to create the final datasets in the Gold Layer.

6. **Verify Data Quality**:
   - Run the `quality_check/quality_check_for_loading_data_into_silver_layer.sql` script to ensure data quality.

7. **Access Analytics and Reports**:
   - Use the generated views in the Gold Layer to access analytics and reports.

For more detailed instructions, refer to the documentation in the `docs/` directory.




