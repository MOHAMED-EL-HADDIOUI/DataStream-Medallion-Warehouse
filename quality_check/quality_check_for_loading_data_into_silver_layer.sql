/*
===============================================================================
Quality Checks for Bronze Layer data before Loading into Silver Layer
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Instructions:
-------------
1. Run each query independently to assess the data quality of the respective tables.
2. Review the results of each query and resolve any data quality issues identified.
3. These checks should be run regularly, especially before loading data into the downstream systems.
===============================================================================
*/

-- -----------------------
-- Table-1: crm_cust_info
-- -----------------------

SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1; -- duplicate check, found, take only unique value

SELECT 
	cst_id
FROM bronze.crm_cust_info
WHERE cst_id IS NULL; -- null check, found, exclude null values


SELECT 
	cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key); -- extra spaces check, no issue found


SELECT 
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname); -- extra spaces check, found, trim column


SELECT 
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname); -- extra spaces check, found, trim column


SELECT 
	cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status IS NULL; -- null check, found, convert null to n/a

SELECT 
	distinct cst_gndr
FROM bronze.crm_cust_info;-- null check, found, convert null to n/a


-- -----------------------
-- Table-2: crm_prd_info
-- -----------------------

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1; -- duplicate check, not found

SELECT prd_id
FROM bronze.crm_prd_info
WHERE prd_id IS NULL; -- null check, not found

SELECT prd_key
FROM bronze.crm_prd_info
WHERE prd_key IS NULL; -- null check, not found

SELECT prd_key
FROM bronze.crm_prd_info
WHERE prd_key != TRIM(prd_key); -- extra spaces check, not found

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm IS NULL; -- null check, not found

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm); -- extra spaces check, not found

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost <1; -- null check, not found

SELECT distinct prd_line
FROM bronze.crm_prd_info
WHERE prd_line IS NULL; -- null check, found, convert NULL to n/a

SELECT prd_start_dt, prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt; -- date anomaly check, issue found, set end date as one day before the next start date

-- -----------------------
-- Table-3: crm_sales_details
-- -----------------------
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num); -- extra spaces check, not found

SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id IS NULL; --  null check, not found

SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_order_dt) !=8; -- issue found, convert anomalies to null

-- no issue found in sls_ship_dt, sls_due_dt

SELECT sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales <=0 OR sls_sales IS NULL; -- issue found, extract data from sls_quantity & sls_price 

SELECT sls_quantity
FROM bronze.crm_sales_details
WHERE sls_quantity <=0 OR sls_quantity IS NULL; -- no issue found

SELECT sls_price
FROM bronze.crm_sales_details
WHERE sls_price <=0 OR sls_price IS NULL; -- issue found, extract data from sls_quantity & sls_sales 


-- -----------------------
-- Table-4: erp_px_cat_g1v2
-- -----------------------

-- no issue found in this table

-- -----------------------
-- Table-5: erp_cust_az12
-- -----------------------
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE(); -- date anomaly check and found, make those date to null that are greater than current date

SELECT DISTINCT gen
FROM bronze.erp_cust_az12; -- issues found in values, normalize those.

-- -----------------------
-- Table-6: erp_loc_a101
-- -----------------------
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101; -- anomaly found, normalize values
