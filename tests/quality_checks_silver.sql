/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

select * from silver.crm_cust_info
-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
select cst_id, count(*) from silver.crm_cust_info group by cst_id having count(*) > 1 or cst_id is null

-- Check for Unwanted Spaces
-- Expectation: No Results
select cst_firstname from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select cst_lastname from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

-- Data Standardization & Consistency
select distinct cst_gndr from silver.crm_cust_info

select distinct cst_marital_status from silver.crm_cust_info

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
select * from silver.crm_prd_info;

select prd_id, count(*) from silver.crm_prd_info group by prd_id having count(*)>1 or prd_id is null
-- Check for Unwanted Spaces
-- Expectation: No Results
select prd_nm from silver.crm_prd_info
where prd_nm != TRIM(prd_nm)
-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
select prd_cost, count(*) from silver.crm_prd_info
group by prd_cost having prd_cost < 0 or prd_cost is null
-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
select * from silver.crm_sales_details
where sls_order_dt >sls_ship_dt or sls_order_dt >sls_due_dt

-- Unwanted spaces
select sls_ord_num from bronze.crm_sales_details
where sls_ord_num != TRIM(sls_ord_num)

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
select distinct sls_sales, sls_quantity, sls_price
from silver.crm_sales_details
where sls_sales !=sls_quantity * sls_price 
or sls_sales is null or sls_quantity is null or sls_price is null

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates before Today
select
cid,
bdate, 
gen 
from silver.erp_cust_az12
where bdate > current_date	
-- Data Standardization & Consistency
select distinct gen from silver.erp_cust_az12
-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;


