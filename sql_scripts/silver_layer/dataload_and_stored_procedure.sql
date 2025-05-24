/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS -- creating stored procedure
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	
	BEGIN TRY
		
		SET @batch_start_time = GETDATE();

		PRINT '===================================';
		PRINT 'Loading Silver Layer';
		PRINT '===================================';
		PRINT '-----------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------';

		/*
		CRM Table
		=================================
		Table 1: silver.crm_cust_info
		=================================
		*/
		
		SET @start_time = GETDATE(); -- fetching loading start time

		PRINT '>> Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> Inserting data into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
			)
 
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,

			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END AS cst_marital_status,

			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date

		FROM (
		SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) AS row_version
		FROM bronze.crm_cust_info) AS T
		WHERE row_version = 1 AND cst_key != 'PO25'; -- removing duplicate unique id

		SET @end_time = GETDATE(); -- fetching loading end time
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' Seconds'; -- calculating load duration
		PRINT '-----------------------------'
			
		/*
		CRM Table
		=================================
		Table 2: silver.crm_prd_info
		=================================
		*/

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>> Inserting data into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
  			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- extract category id
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- extract product key
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,

			CASE
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- map product line to descriptive values

			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			-- calculate end date as one day before the next start date
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
    
		FROM bronze.crm_prd_info;

		SET @end_time = GETDATE(); -- fetching loading end time
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' Seconds'; -- calculating load duration
		PRINT '-----------------------------'


		/*
		CRM Table
		=================================
		Table 3: silver.crm_sales_details
		=================================
		*/
		SET @start_time = GETDATE();

		PRINT '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> Inserting data into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)

		SELECT * FROM (
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
	
				CASE 
					WHEN LEN(sls_order_dt) !=8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
				END AS sls_order_dt,

				CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) AS sls_ship_dt,
				CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) AS sls_due_dt,

				CASE 
					WHEN sls_sales != sls_quantity*sls_price OR sls_sales <=0 OR sls_sales IS NULL 
						THEN sls_quantity*ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales,

				sls_quantity,

				CASE 
					WHEN sls_price != sls_sales/sls_quantity OR sls_price <=0 OR sls_price IS NULL 
						THEN ABS(sls_sales)/sls_quantity
					ELSE sls_price
				END AS sls_price
			FROM bronze.crm_sales_details) t where sls_sales = sls_price/sls_quantity

		UNION ALL

		SELECT
			sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_sales*sls_quantity AS sls_price
	
		FROM (
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
	
				CASE 
					WHEN LEN(sls_order_dt) !=8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
				END AS sls_order_dt,

				CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) AS sls_ship_dt,
				CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) AS sls_due_dt,

				CASE 
					WHEN sls_sales != sls_quantity*sls_price OR sls_sales <=0 OR sls_sales IS NULL 
						THEN sls_quantity*ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales,

				sls_quantity,

				CASE 
					WHEN sls_price != sls_sales/sls_quantity OR sls_price <=0 OR sls_price IS NULL 
						THEN ABS(sls_sales)/sls_quantity
					ELSE sls_price
				END AS sls_price
			FROM bronze.crm_sales_details) t where sls_sales != sls_price/sls_quantity;

		SET @end_time = GETDATE(); -- fetching loading end time
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' Seconds'; -- calculating load duration
		PRINT '-----------------------------'


		/*
		ERP Table
		=================================
		Table 4: silver.erp_cust_az12
		=================================
		*/
				
		PRINT '-----------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE()

		PRINT '>> Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>> Inserting data into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)

		SELECT
			CASE 
				WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(CID)) -- remove NAS prefix if present
			ELSE cid
			END AS cid,
	
			CASE 
				WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
			END AS bdate, -- set future birth dates to NULL
	
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- normalize gender values and handle unknown cases

		from bronze.erp_cust_az12;

		SET @end_time = GETDATE(); -- fetching loading end time
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' Seconds'; -- calculating load duration
		PRINT '-----------------------------'

		/*
		ERP Table
		=================================
		Table 5: silver.erp_loc_a101
		=================================
		*/

		SET @start_time = GETDATE();

		PRINT '>> Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> Inserting data into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
			)
		SELECT 
			REPLACE(cid,'-','') AS cid,
			CASE
				WHEN TRIM(cntry) LIKE 'US%' THEN 'United States'
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry

		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE(); -- fetching loading end time
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' Seconds'; -- calculating load duration
		PRINT '-----------------------------'


		/*
		ERP Table
		=================================
		Table 6: silver.erp_px_cat_g1v2
		=================================
		*/

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>> Inserting data into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE(); -- fetching loading end time
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' Seconds'; -- calculating load duration
		PRINT '-----------------------------'

		SET @batch_end_time = GETDATE();
		-- calculating silver layer load duration
		PRINT '========================='
		PRINT 'Loading Silver Layer is Completed'
		PRINT 'Silver Layer Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + ' Seconds' 
		PRINT '========================='

	END TRY

	BEGIN CATCH
		PRINT '========================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Message:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message:' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '========================='
	END CATCH

END;
