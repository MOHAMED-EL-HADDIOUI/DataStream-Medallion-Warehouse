/*
==========================================================
📥 BRONZE LAYER DATA LOAD SCRIPT
==========================================================

📌 Summary:
This script loads raw data into the Bronze Layer tables of the 
DataStream Medallion Warehouse. It uses source files (e.g., CSVs) or 
external data connections to populate the staging tables created 
in the Bronze schema.

✅ Purpose:
- Ingest data from external sources into the raw layer
- Preserve original structure and values without transformation
- Enable auditability and replayability by storing data as-is

⚠️ Notes:
- Assumes that Bronze Layer tables have already been created
- No business logic, cleaning, or enrichment should be applied
- File paths, formats, and delimiters must match the source files

📁 Layer: Bronze (Raw / Staging)

----------------------------------------------------------
Begin data load commands below
----------------------------------------------------------
*/



USE bronze;

SET @batch_start_time = NOW();

	/*
	CRM Table
	=================================
	Table 1: bronze.crm_cust_info
	=================================
	*/

	SET @start_time = NOW(); 
    SET @table_loaded = 0;

		TRUNCATE TABLE crm_cust_info;
	
	SET @before = (SELECT COUNT(*) FROM crm_cust_info);
	
		LOAD DATA LOCAL INFILE 'C:/Users/psdcp/Desktop/Data warehouse project/datasets/source_crm/cust_info.csv'
		INTO TABLE crm_cust_info
		FIELDS TERMINATED BY ',' 
		IGNORE 1 ROWS;
	SET @after = (SELECT COUNT(*) FROM crm_cust_info);
    SET @table_loaded = @table_loaded + 1;
	SET @end_time = NOW();

	-- printing messages
	SELECT 
		'Truncating table: crm_cust_info' AS operation,
		'Loading data into table: crm_cust_info' AS operation,
		CONCAT(@after - @before, ' rows') AS row_loaded,
		TIMEDIFF(@end_time, @start_time) AS load_duration;


	/*
	CRM Table
	=================================
	Table-2: crm_prd_info
	=================================
	*/

	SET @start_time = NOW();

		TRUNCATE TABLE crm_prd_info;

	SET @before = (SELECT COUNT(*) FROM crm_prd_info);

		LOAD DATA LOCAL INFILE 'C:/Users/psdcp/Desktop/Data warehouse project/datasets/source_crm/prd_info.csv'
		INTO TABLE crm_prd_info
		FIELDS TERMINATED BY ','
		IGNORE 1 ROWS;
	SET @after = (SELECT COUNT(*) FROM crm_prd_info);
    SET @table_loaded = @table_loaded + 1;
	SET @end_time = NOW();
	-- printing messages
	SELECT
		'Truncating table: crm_prd_info' AS operation,
		'Loading data into table: crm_prd_info' AS operation,	
		CONCAT(@after - @before,' rows') AS row_loaded,
		TIMEDIFF(@end_time, @start_time) AS load_duration;

	/*
	CRM Table
	=================================
	Table-3: crm_sales_details
	=================================
	*/
	SET @start_time = NOW();

		TRUNCATE TABLE crm_sales_details;
		
	SET @before = (SELECT COUNT(*) FROM crm_sales_details);

		LOAD DATA LOCAL INFILE 'C:/Users/psdcp/Desktop/Data warehouse project/datasets/source_crm/sales_details.csv'
		INTO TABLE crm_sales_details
		FIELDS TERMINATED BY ','
		IGNORE 1 ROWS;
	SET @after = (SELECT COUNT(*) FROM crm_sales_details);
    SET @table_loaded = @table_loaded + 1;
	SET @end_time = NOW();

	-- printing messages
	SELECT 
		'Truncating table: crm_sales_details' AS operation,
		'Loading data into table: crm_sales_details' AS operation,
		CONCAT(@after - @before,' rows') AS row_loaded,
		TIMEDIFF(@end_time, @start_time) AS load_duration;

	/*
	ERP Table
	=================================
	Table-1: erp_cust_az12
	=================================
	*/
	SET @start_time = NOW();
		TRUNCATE TABLE erp_cust_az12;

	SET @before = (SELECT COUNT(*) FROM erp_cust_az12);
		LOAD DATA LOCAL INFILE 'C:/Users/psdcp/Desktop/Data warehouse project/datasets/source_erp/cust_az12.csv'
		INTO TABLE erp_cust_az12
		FIELDS TERMINATED BY ','
		IGNORE 1 ROWS;

	SET @after = (SELECT COUNT(*) FROM erp_cust_az12);
    SET @table_loaded = @table_loaded + 1;
	SET @end_time = NOW();

	SELECT
		'Truncating table: erp_cust_az12' AS operation,
		'Loading data into table: erp_cust_az12' AS operation,
		CONCAT(@after - @before,' rows') AS row_loaded,
		TIMEDIFF(@end_time, @start_time) AS load_duration;
		

	/*
	ERP Table
	=================================
	Table-2: erp_loc_a101
	=================================
	*/
	SET @start_time = NOW();
	TRUNCATE TABLE erp_loc_a101;

	SET @before = (SELECT COUNT(*) FROM erp_loc_a101);
		LOAD DATA LOCAL INFILE 'C:/Users/psdcp/Desktop/Data warehouse project/datasets/source_erp/loc_a101.csv'
		INTO TABLE erp_loc_a101
		FIELDS TERMINATED BY ','
		IGNORE 1 ROWS;
	SET @after = (SELECT COUNT(*) FROM erp_loc_a101);
    SET @table_loaded = @table_loaded + 1;
	SET @end_time = NOW();
	SELECT 
		'Truncating table: erp_loc_a101' AS operation,
		'Loading data into table: erp_loc_a101' AS operation,
		CONCAT(@after - @before, ' rows') AS row_loaded,
		TIMEDIFF(@end_time, @start_time) AS load_duration;

	/*
	ERP Table
	=================================
	Table-3: erp_px_cat_g1v2
	=================================
	*/
	SET @start_time = NOW();
		TRUNCATE TABLE erp_px_cat_g1v2;

	SET @before = (SELECT COUNT(*) FROM erp_px_cat_g1v2);
		LOAD DATA LOCAL INFILE 'C:/Users/psdcp/Desktop/Data warehouse project/datasets/source_erp/px_cat_g1v2.csv'
		INTO TABLE erp_px_cat_g1v2
		FIELDS TERMINATED BY ','
		IGNORE 1 ROWS;
	SET @after = (SELECT COUNT(*) FROM erp_px_cat_g1v2);
    SET @table_loaded = @table_loaded + 1;
	SET @end_time = NOW();

	SELECT 
		'Truncating table: erp_px_cat_g1v2' AS operation,
		'Loading data into table: erp_px_cat_g1v2' AS operation,
		CONCAT(@after - @before, ' rows') AS row_loaded,
		TIMEDIFF(@end_time, @start_time) AS load_duration;

SET @batch_end_time = NOW();

SELECT 
	'Bronze layer loading completed' AS operations,
    @table_loaded AS total_table_loaded,
    TIMEDIFF(@batch_end_time, @batch_start_time) AS load_duration;
    
