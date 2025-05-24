/*
===============================================================================
Stored Procedure: Load Bronze Layer 
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

Note: if you use this code do not forget to change the source location of csv files.
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS -- store procedure
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; -- declaring variables
	BEGIN TRY	-- error detection
	SET @batch_start_time = GETDATE();
		PRINT '===================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===================================';
		PRINT '-----------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------';

		/*
		CRM Table
		=================================
		Table 1: bronze.crm_cust_info
		=================================
		*/
		
		SET @start_time = GETDATE();	-- fetching loading start time
		
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\psdcp\Desktop\Data warehouse project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE();		-- fetching laoding end time
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'; -- calculating loading duration and print to output
		PRINT '>> ------------------';

		/*
		CRM Table
		=================================
		Table 2: bronze.crm_prd_info
		=================================
		*/
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table:  bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		from 'C:\Users\psdcp\Desktop\Data warehouse project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		/*
		CRM Table
		=================================
		Table 3: bronze.crm_sales_details
		=================================
		*/
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		from 'C:\Users\psdcp\Desktop\Data warehouse project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';


				
		PRINT '-----------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------';

		/*
		ERP Table
		=================================
		Table 1: bronze.erp_cust_az12
		=================================
		*/

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		from 'C:\Users\psdcp\Desktop\Data warehouse project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		/*
		ERP Table
		=================================
		Table 2: bronze.erp_loc_a101
		=================================
		*/
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		from 'C:\Users\psdcp\Desktop\Data warehouse project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		/*
		ERP Table
		=================================
		Table 3: bronze.erp_loc_a101
		=================================
		*/

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'C:\Users\psdcp\Desktop\Data warehouse project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>> ------------------';

		SET @batch_end_time = GETDATE();
		PRINT '========================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
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

END
