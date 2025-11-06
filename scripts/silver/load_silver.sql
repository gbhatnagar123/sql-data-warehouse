/*
Stored Procedure: Load Silver Layer (Bronze -> Silver)
Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process to
populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleaned data from Bronze into Silver tables.
Parameters:
None.
This stored procedure does not accept any parameters or return any values.
Usage Example:
call Silver.load_silver();
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
    ----------------------------------------------------------------------
    -- Load: silver.crm_cust_info (Customer Information)
    -- Deduplication: Selects the latest record based on cst_create_date.
    -- Transformation: Standardizes marital status and gender codes.
    ----------------------------------------------------------------------

	RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;

	RAISE NOTICE '>> Inserting data into silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
	SELECT 
        cst_id, 
        cst_key, 
        trim(cst_firstname) AS cst_firstname, 
        trim(cst_lastname) AS cst_lastname,
        CASE 
            WHEN upper(trim(cst_marital_status)) = 'S' THEN 'Single'
            WHEN upper(trim(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
            WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
	FROM (
        SELECT 
            *, 
            -- Flag the last (latest) record for each customer ID
            row_number() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
        FROM bronze.crm_cust_info 
	) AS subquery
    WHERE flag_last = 1;


    ----------------------------------------------------------------------
    -- Load: silver.crm_prd_info (Product Information)
    -- Transformation: Derives cat_id, cleans prd_key, handles null cost, 
    --                 standardizes prd_line, and computes prd_end_dt using LEAD.
    ----------------------------------------------------------------------

	RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;

	RAISE NOTICE '>> Inserting data into silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
	SELECT 
        prd_id,
        replace(substring(prd_key, 1, 5), '-', '_') AS cat_id,
        substring(prd_key, 7, length(prd_key)) AS prd_key, -- From 7th element to the end of prd key
        prd_nm,
        CASE 
            WHEN prd_cost IS NULL THEN 0
            ELSE prd_cost
        END AS prd_cost,
        CASE upper(trim(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        prd_start_dt,
        -- Calculate end date as the day before the next start date for the same product key
        lead(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) - INTERVAL '1 day' AS prd_end_dt
	FROM bronze.crm_prd_info;


    ----------------------------------------------------------------------
    -- Load: silver.crm_sales_details (Sales Transactions)
    -- Transformation: Validates and converts date columns (assuming YYYYMMDD text/int format).
    --                 Performs data cleansing/derivation for sales and price values.
    ----------------------------------------------------------------------

	RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;

	RAISE NOTICE '>> Inserting data into silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(
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
	SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Convert sls_order_dt from potential INT/TEXT (YYYYMMDD) to DATE
        CASE 
            WHEN sls_order_dt <= 0 OR length(sls_order_dt ::text) != 8 THEN NULL
            ELSE to_date(sls_order_dt ::text, 'YYYYMMDD')
        END AS sls_order_dt,
        -- Convert sls_ship_dt from potential INT/TEXT (YYYYMMDD) to DATE
        CASE 
            WHEN sls_ship_dt <= 0 OR length(sls_ship_dt ::text) != 8 THEN NULL
            ELSE to_date(sls_ship_dt ::text, 'YYYYMMDD')
        END AS sls_ship_dt,
        -- Convert sls_due_dt from potential INT/TEXT (YYYYMMDD) to DATE
        CASE 
            WHEN sls_due_dt <= 0 OR length(sls_due_dt ::text) != 8 THEN NULL
            ELSE to_date(sls_due_dt ::text, 'YYYYMMDD')
        END AS sls_due_dt,
        -- Cleanse/Derive sls_sales
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity
            THEN sls_quantity * abs(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        -- Cleanse/Derive sls_price
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
	FROM bronze.crm_sales_details;


    ----------------------------------------------------------------------
    -- Load: silver.erp_cust_az12 (ERP Customer Data)
    -- Transformation: Removes 'NAS' prefix from CID, validates birth dates, 
    --                 and standardizes gender.
    ----------------------------------------------------------------------

	RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;

	RAISE NOTICE '>> Inserting data into silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
	SELECT
        CASE 
            WHEN cid LIKE 'NAS%' THEN substring(cid, 4, length(cid))
            ELSE cid
        END AS cid,
        CASE 
            WHEN bdate > current_date THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN upper(trim(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN upper(trim(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a' 
        END AS gen
	FROM bronze.erp_cust_az12;


    ----------------------------------------------------------------------
    -- Load: silver.erp_loc_a101 (ERP Location Data)
    -- Transformation: Cleans CID (removes hyphens) and standardizes country names.
    ----------------------------------------------------------------------

	RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;

	RAISE NOTICE '>> Inserting data into silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(cid, cntry)
	SELECT 
        replace(cid, '-', '') AS cid,
        CASE 
            WHEN trim(cntry) = 'DE' THEN 'Germany'
            WHEN trim(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN trim(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE trim(cntry)
        END AS cntry	
	FROM bronze.erp_loc_a101;


    ----------------------------------------------------------------------
    -- Load: silver.erp_px_cat_g1v2 (ERP Product Category)
    -- Transformation: Simple passthrough (assumes no complex transformation needed).
    ----------------------------------------------------------------------

	RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	RAISE NOTICE '>> Inserting data into silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
	SELECT
        id,
        cat,
        subcat,
        maintenance
	FROM bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Silver layer load completed successfully.';

END;
$$;

call silver.load_silver()
