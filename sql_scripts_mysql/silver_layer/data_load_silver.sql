/*
==========================================================
🥈 SILVER LAYER DATA TRANSFORMATION & LOAD SCRIPT
==========================================================

📌 Summary:
This script performs the transformation and loading of cleansed data 
from the Bronze Layer into the Silver Layer of the DataStream Medallion Warehouse. 
It applies business rules, standardizes formats, ensures consistency, 
and prepares the data for downstream consumption.

✅ Purpose:
- Load data from Bronze into corresponding Silver Layer tables
- Apply calculated fields, corrections, and business logic
- Ensure type safety, formatting, and referential integrity
- Prepare structured datasets for the Gold Layer and reporting

⚙️ Key Transformations:
- Date formatting and conversion (e.g., CAST, STR_TO_DATE)
- Numeric validation and calculations (e.g., price = sales * quantity)
- Blank/null handling (e.g., COALESCE, CASE WHEN)
- Deduplication using ROW_NUMBER, GROUP BY
- Text standardization and cleanup (e.g., TRIM, REPLACE, UPPER/LOWER)

📁 Layer: Silver (Cleansed / Transformed)

🚫 Note:
- Ensure Bronze Layer data is cleaned before loading
- Validate results with SELECT before final INSERT or UPDATE
- Maintain schema alignment with the Silver Layer table definitions

----------------------------------------------------------
Begin transformation and load logic below
----------------------------------------------------------
*/



USE silver;

/*
CRM Table
=================================
Table 1: silver.crm_cust_info
=================================
*/

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
	cst_id, 
    cst_key, 
    cst_firstname, 
    cst_lastname, 
    cst_marital_status, 
    cst_gndr, 
    cst_create_date)	

SELECT 
	cst_id, 
    cst_key, 
    TRIM(cst_firstname) AS cst_firstname, 
    TRIM(cst_lastname) AS cst_lastname, 
    
    CASE
		WHEN TRIM(cst_marital_status) = 'M' THEN 'Married'
        WHEN TRIM(cst_marital_status) = 'S' THEN 'Single'
        ELSE 'n/a'
	END AS cst_marital_status,
    
    CASE
		WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
        WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
        ELSE 'n/a'
	END AS cst_gndr, 
    
    cst_create_date
    
FROM 
(SELECT *,
	ROW_NUMBER() over(PARTITION BY cst_id ORDER BY cst_create_date DESC) as row_count
from bronze.crm_cust_info)t
WHERE row_count = 1 AND cst_id !=0
;

/*
CRM Table
=================================
Table 2: silver.crm_prd_info
=================================
*/

TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (
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
    REPLACE (TRIM(LEFT (prd_key,5)),'-','_') AS cat_id,
	TRIM(RIGHT(RIGHT(prd_key,LENGTH(prd_key)-5),LENGTH(RIGHT(prd_key,LENGTH(prd_key)-5))-1)) AS prd_key,
    TRIM(prd_nm), 
    prd_cost, 
    
	CASE 
		WHEN TRIM(prd_line) = 'M' THEN 'Mountain'
		WHEN TRIM(prd_line) = 'R' THEN 'Road'
		WHEN TRIM(prd_line) = 'S' THEN 'Other Sales'
		WHEN TRIM(prd_line) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
    
    CAST(prd_start_dt AS DATE) AS prd_start_dt, 
    DATE_SUB(CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE), INTERVAL 1 DAY) AS prd_end_dt
    
FROM bronze.crm_prd_info;

/*
CRM Table  
=================================
Table 3: silver.crm_sales_details
=================================
*/
TRUNCATE silver.crm_sales_details;
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

SELECT *
FROM (
SELECT
	sls_ord_num, 
    sls_prd_key, 
    sls_cust_id, 
    CASE 
		WHEN LENGTH(sls_order_dt) = 8 THEN CAST(sls_order_dt AS DATE)
        ELSE NULL
	END AS sls_order_dt,
    CAST(sls_ship_dt AS DATE) AS sls_ship_dt, 
    CAST(sls_due_dt AS DATE) AS sls_due_dt, 
    
	  CASE 
		when sls_sales <=0 then abs(sls_price)/sls_quantity
        else sls_sales
	  END AS sls_sales,
    
    sls_quantity,
    case
		when sls_price <= 0 then sls_sales*sls_quantity
        else sls_price
	end as sls_price
		
    
FROM bronze.crm_sales_details) t where sls_price = sls_sales*sls_quantity

UNION ALL

select 
	sls_ord_num, 
    sls_prd_key, 
    sls_cust_id, 
    sls_order_dt, 
    sls_ship_dt, 
    sls_due_dt, 
    sls_sales, 
    sls_quantity,
	round(sls_sales*sls_quantity,0) as sls_price
from (
SELECT
	sls_ord_num, 
    sls_prd_key, 
    sls_cust_id, 
    CASE 
		WHEN LENGTH(sls_order_dt) = 8 THEN CAST(sls_order_dt AS DATE)
        ELSE NULL
	END AS sls_order_dt,
    
    CAST(sls_ship_dt AS DATE) AS sls_ship_dt, 
    CAST(sls_due_dt AS DATE) AS sls_due_dt, 
    
	  CASE 
		when sls_sales <=0 then abs(sls_price)/sls_quantity
        else sls_sales
	  END AS sls_sales,
    
    sls_quantity,
    case
		when sls_price <= 0 then sls_sales*sls_quantity
        else sls_price
	end as sls_price
		
FROM bronze.crm_sales_details) t where sls_price != sls_sales*sls_quantity;

/*
ERP Table  
=================================
Table 1: silver.erp_cust_az12
=================================
*/
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12(
	cid,
    bdate,
    gen
    )
SELECT 
	RIGHT(cid,(LENGTH(cid)-3)) cid,
    
    CASE 
		WHEN bdate >= NOW() THEN NULL
        ELSE bdate
	END AS bdate, 
    
  CASE 
    WHEN LOWER(TRIM(REPLACE(REPLACE(REPLACE(gen, '\r', ''), '\n', ''), '\t', ''))) = 'm' THEN 'Male'
    WHEN LOWER(TRIM(REPLACE(REPLACE(REPLACE(gen, '\r', ''), '\n', ''), '\t', ''))) = 'f' THEN 'Female'
    WHEN gen IS NULL THEN 'n/a'
    WHEN gen REGEXP '^[[:space:]]*$' THEN 'n/a'
    ELSE TRIM(REPLACE(REPLACE(REPLACE(gen, '\r', ''), '\n', ''), '\t', ''))
  END AS gen
FROM bronze.erp_cust_az12;

/*
ERP Table  
=================================
Table 2: silver.erp_loc_a101
=================================
*/

TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 (
	cid,
    cntry
    )

SELECT 
	cid,
    CASE 
		WHEN cntry LIKE 'US%' THEN 'United States'
        WHEN cntry = 'DE' THEN 'Germany'
        ELSE cntry
	END AS cntry
FROM (
SELECT 
	REPLACE(cid,'-','') as cid,
    CASE 
        WHEN cntry REGEXP '^[[:space:]]*$' THEN 'n/a'
        ELSE TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''))
	END AS cntry
FROM bronze.erp_loc_a101) t;

/*
ERP Table  
=================================
Table 3: silver.erp_px_cat_g1v2
=================================
*/

TRUNCATE TABLE silver.erp_px_cat_g1v2;
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
    
	CASE 
		WHEN TRIM(REPLACE(REPLACE(maintenance, '\r', ''), '\n', '')) = '' THEN 'N/A'
		ELSE TRIM(REPLACE(REPLACE(maintenance, '\r', ''), '\n', ''))
	END AS maintenance
  
FROM bronze.erp_px_cat_g1v2;
