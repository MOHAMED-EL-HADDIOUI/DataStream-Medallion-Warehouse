
/*
Bronze Layer Data Cleaning Script
------------------------------------
✅ Purpose:
This script performs standardized data cleaning across all Bronze Layer tables
in the data warehouse pipeline. It prepares raw ingested data for the Silver Layer
by applying general data hygiene, deduplication, and transformation logic.

✅ Scope:
Applies to all raw/landing tables in the Bronze layer. 
Designed to clean fields related to identifiers, categories, dates, numerics, and strings.

✅ Key Transformations Applied:
- Remove line breaks: REPLACE('\r', ''), REPLACE('\n', '')
- Trim whitespace: TRIM()
- Replace blank or whitespace-only strings with 'n/a'
- Standardize categorical values (e.g., gender codes, country names)
- Validate and correct numeric columns (e.g., price, sales, quantity)
- Handle invalid or malformed date fields
- Use CASE WHEN logic to apply conditional replacements or calculations
- Remove non-printable or unwanted characters (e.g., tabs, symbols)
- Normalize text casing if necessary (UPPER/LOWER functions)
- Detect and remove duplicate records while it was necessary

*/

-- -----------------------
-- Table-1: crm_cust_info
-- -----------------------

SELECT 
cst_id, count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) !=1 -- checking duplicate values, found, keep only unique values
;

SELECT 
cst_id
FROM bronze.crm_cust_info
WHERE cst_id = 0 OR cst_id = '' OR cst_id is NULL; -- null or blank or 0 value checking, found, exclude 0


SELECT 
cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key); -- extra space check, not found


SELECT 
cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname); -- extra space check, found, perform trim operation


SELECT 
cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname); -- extra space check, found, perform trim operation


SELECT
cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status IS NULL OR cst_marital_status = '' ; -- checking null or blank values, found, convert blank of null to n/a


SELECT
cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr IS NULL OR cst_gndr = ''; -- -- checking null or blank values, found, convert blank of null to n/a

-- -----------------------
-- Table-2: crm_prd_info
-- -----------------------

SELECT 
prd_id, count(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) !=1; -- checking duplicate values, not found

SELECT 
prd_id
FROM bronze.crm_prd_info
WHERE prd_id != TRIM(prd_id); -- extra spaces check, not found

SELECT 
prd_id
FROM bronze.crm_prd_info
WHERE prd_id IS NULL OR prd_id = ''; -- null or blank value check, not found

SELECT 
prd_key
FROM bronze.crm_prd_info
WHERE prd_key != TRIM(prd_key); -- extra spaces check, not found

SELECT 
prd_key
FROM bronze.crm_prd_info
WHERE prd_key IS NULL OR prd_key = ''; -- null or blank value check, not found

SELECT 
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm IS NULL OR prd_nm = ''; -- null or blank value check, not found

SELECT 
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm); -- extra spaces check, not found

SELECT 
prd_line
FROM bronze.crm_prd_info
WHERE prd_line IS NULL OR prd_line = ''; -- null or blank value check, found, convert blank to n/a

SELECT 
prd_start_dt
FROM bronze.crm_prd_info
WHERE prd_start_dt>prd_end_dt ; -- date anomaly check, issue found, set end date as one day before the next start date


SELECT prd_start_dt
FROM bronze.crm_prd_info
WHERE CAST(prd_start_dt AS DATE) IS NULL 
	OR CAST(prd_start_dt AS DATE) = 0000-00-00; -- no issue found

-- -----------------------
-- Table-3: crm_prd_info
-- -----------------------
select *
from bronze.crm_sales_details;

SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num); -- extra spaces check, no issue found

SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NULL OR sls_ord_num = ''; -- null check, no issue found

SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key); -- extra spaces check, no issue found

SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key IS NULL OR sls_prd_key = ''; -- null check, no issue found

SELECT sls_due_dt
FROM bronze.crm_sales_details
where length(sls_due_dt)!=8;

-- correct the date format of sls_order_dt, sls_ship_dt, sls_due_dt

SELECT sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales = '' OR sls_sales <=0; -- issues found, fix this

SELECT sls_quantity
FROM bronze.crm_sales_details
WHERE sls_quantity IS NULL OR sls_quantity = ''; -- no issue found

SELECT sls_price
FROM bronze.crm_sales_details
WHERE sls_price IS NULL OR sls_price = '' or sls_price <=0; -- issues found, fix this

-- -----------------------
-- Table-4: erp_cust_az12
-- -----------------------
SELECT cid
FROM bronze.erp_cust_az12
WHERE cid != TRIM(cid); -- extra spaces check, no issue found

SELECT cid
FROM bronze.erp_cust_az12
WHERE cid IS NULL OR cid = '';

SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate is null or bdate = 0000-00-00; -- null check, not found

SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate >= now(); -- date anomaly check, found, convert those to null

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

SELECT distinct gen, length(gen)
FROM bronze.erp_cust_az12; -- issues found, normalize these

-- -----------------------
-- Table-5: erp_loc_a101
-- -----------------------
SELECT cid
FROM bronze.erp_loc_a101
WHERE cid != TRIM(cid); -- extra spaces check, no issues found

SELECT cid
FROM bronze.erp_loc_a101
WHERE cid REGEXP '^[[:space:]]*$'; -- no issue found

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101; -- issues found, normalize these

-- -----------------------
-- Table-6: erp_px_cat_g1v2
-- -----------------------
select * from
bronze.erp_px_cat_g1v2;

SELECT id 
FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM(id); -- no issue found

SELECT id 
FROM bronze.erp_px_cat_g1v2
WHERE id IS NULL OR id = ''; -- no issue found

select distinct cat
from bronze.erp_px_cat_g1v2; -- no issue found

select distinct subcat
from bronze.erp_px_cat_g1v2 
order by subcat; -- no issue found

select distinct maintenance, length(maintenance)
from bronze.erp_px_cat_g1v2 ; -- issues found, normalize these 

