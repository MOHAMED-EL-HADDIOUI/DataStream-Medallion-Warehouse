/*
==========================================================
🥇 GOLD LAYER DEVELOPMENT SCRIPT
==========================================================

📌 Summary:
This script is responsible for generating business-ready datasets 
from the cleansed and standardized data stored in the Silver Layer. 
The Gold Layer contains aggregated, enriched, and analytics-optimized 
data structures for use in reporting, dashboards, and advanced analytics.

✅ Purpose:
- Deliver final curated datasets to support decision-making
- Apply business logic, aggregations, and advanced calculations
- Serve as the single source of truth for BI tools and end-users

📁 Layer: Gold (Business-Ready / Analytics Layer)

🚫 Note:
- Data in this layer is derived through transformations from the Silver Layer
- Views in this layer should be optimized for analytical performance
- Naming convention: [schema].[subject_area]_[business_entity]

*/


USE gold;

-- =======================================
-- Create Dimension: gold.dim_customers
-- =======================================

DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS 
SELECT 
    ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
    ci.cst_id 				AS customer_id, 
    ci.cst_key 				AS customer_number, 
    ci.cst_firstname 		AS first_name, 
    ci.cst_lastname 		AS last_name, 
    cl.cntry 				AS country,
    ci.cst_marital_status 	AS marital_status, 
    
    CASE 
        WHEN ec.gen = 'n/a' OR ec.gen IS NULL THEN ci.cst_gndr
        ELSE ec.gen
    END AS gender,
    
    ec.bdate 				AS birth_date,
    ci.cst_create_date 		AS create_date
FROM 
    silver.crm_cust_info ci
LEFT JOIN 
    silver.erp_cust_az12 ec
    ON ci.cst_key = ec.cid
LEFT JOIN 
    silver.erp_loc_a101 cl
    ON ci.cst_key = cl.cid;
    
-- =======================================
-- Create Dimension: gold.dim_products
-- =======================================

DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS 
SELECT
	ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
	pi.prd_id 		AS product_id, 
    pi.prd_key 		AS product_number,
    pi.prd_nm 		AS product_name,
    pc.id 			AS category_id,
    pc.cat 			AS category, 
    pc.subcat 		AS subcategory,
    pc.maintenance,
    pi.prd_cost 	AS cost, 
    pi.prd_line 	AS product_line, 
    pi.prd_start_dt AS start_date
 
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pi.cat_id = pc.id
    WHERE prd_end_dt IS NULL;

-- =======================================
-- Create Fact Table: gold.fact_sales
-- =======================================
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS 
SELECT 
	sd.sls_ord_num 	AS order_number, 
    gp.product_key, 
    gc.customer_key,
    sd.sls_order_dt AS order_date, 
    sd.sls_ship_dt 	AS ship_date, 
    sd.sls_due_dt 	AS due_date, 
    sd.sls_sales 	AS sales_amount, 
    sd.sls_quantity AS quantity, 
    sd.sls_price 	AS price
  
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products gp
	ON sd.sls_prd_key = gp.product_number
LEFT JOIN gold.dim_customers gc
	ON sd.sls_cust_id = gc.customer_id;
