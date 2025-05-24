/*
==========================================================
🧱 BRONZE LAYER TABLE CREATION SCRIPT
==========================================================

📌 Summary:
This script defines the structure of all raw data tables in the 
Bronze Layer of the DataStream Medallion Warehouse. It creates empty 
tables that mirror the schema of the source systems, acting as the 
first landing zone for ingested data.

✅ Purpose:
- Serve as raw, unprocessed storage of source data
- Maintain high fidelity to the original format for traceability
- Provide a foundation for downstream cleansing (Silver) and analytics (Gold)

🚫 Note:
- This script does NOT include any data loading logic
- Data ingestion is handled separately through ETL/ELT processes
- Naming convention: [schema].[source]_[entity]

📁 Layer: Bronze (Raw / Staging)

----------------------------------------------------------
Begin table creation statements below
----------------------------------------------------------
*/



USE bronze;

/*
CRM Table creation
---------------------
Table-1: crm_cust_info
Table-2: crm_prd_info
Table-3: crm_sales_details
*/

-- TABLE-1: crm_cust_info
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id 				INT,
	cst_key 			VARCHAR(50),
	cst_firstname 		VARCHAR(50),
	cst_lastname 		VARCHAR(50),
	cst_marital_status 	VARCHAR(50),
	cst_gndr 			VARCHAR(50),
	cst_create_date 	DATE
	);

-- Table-2: crm_prd_info

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id 			INT,
	prd_key 		VARCHAR(50),
	prd_nm 			VARCHAR(50),
	prd_cost 		INT,
	prd_line 		VARCHAR(50),
	prd_start_dt 	DATE,
	prd_end_dt 		DATE 
	);

-- Table-3: crm_sales_details
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num 	VARCHAR(50),
	sls_prd_key 	VARCHAR(50),
	sls_cust_id 	VARCHAR(50),
	sls_order_dt	INT,
	sls_ship_dt 	INT,
	sls_due_dt 		INT,
	sls_sales 		INT,
	sls_quantity 	INT,
	sls_price 		INT
	);

/*
ERP Table creation
---------------------
Table-1: erp_cust_az12
Table-2: erp_loc_a101
Table-3: erp_px_cat_g1v2
*/

-- Table-1: erp_cust_az12

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
	cid 	VARCHAR(50),
    bdate 	DATE, 
    gen 	VARCHAR(50)
    );

-- Table-2: erp_loc_a101
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid 	VARCHAR(50),
    cntry 	VARCHAR(50)
    );

-- Table-3: erp_px_cat_g1v2
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id 			VARCHAR(50),
    cat 		VARCHAR(50),
    subcat 		VARCHAR(50),
    maintenance VARCHAR(50)
    );

