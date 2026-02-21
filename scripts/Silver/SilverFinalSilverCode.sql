CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    BEGIN TRY
        TRUNCATE TABLE silver.crm_cust_info
        PRINT '>>>>> INSERTING DATA INTO: silver.crm_cust_info'
        INSERT
        INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT cst_id,
               cst_key,
               TRIM(cst_firstname)                                                                        cst_firstname,
               TRIM(cst_lastname)                                                                         cst_lastname,
               CASE WHEN cst_material_status IS NULL THEN 'N/A' ELSE UPPER(TRIM(cst_material_status)) END cst_gndr,
               ISNULL(cst_gndr, 'N/A') cst_gndr,
               cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
            FROM bronze.crm_cust_info) t
        WHERE flag_last = 1

        TRUNCATE TABLE silver.crm_prd_info
        PRINT '>>>>> INSERTING DATA INTO: silver.crm_prd_info'
        INSERT
        INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt)
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id, --erp_px_cat_g1v2 has an _ while this table has a -, so we need to replace it to be consistent
            SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key, --we need the other part of the product key in order to join it with the table crm_sales_details
            prd_nm,
            ISNULL(prd_cost, 0) prd_cost, --checking for nulls in the cost
            ISNULL(prd_line, 'N/A') prd_line, --replacing NULLs here too
            CAST(prd_start_dt AS DATE) prd_start_dt,
            CAST(
                    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
                AS DATE) prd_end_dt --Calculate end date as one day before the next start date
        --this is just to ensure consistency between the dates so they dont overlap, actual project requirements may vary
        FROM bronze.crm_prd_info

        TRUNCATE TABLE silver.crm_sales_details
        PRINT '>>>>> INSERTING DATA INTO: silver.crm_sales_details'
        INSERT
        INTO silver.crm_sales_details (
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
            CASE
                WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
                    THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END sls_order_dt,
            CASE
                WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8
                    THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END sls_ship_dt,
            CASE
                WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8
                    THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END sls_due_dt,
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END sls_sales, --in accordance to the rules outlined above we transform the sales and price
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price = 0
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END sls_price
        FROM bronze.crm_sales_details

        TRUNCATE TABLE silver.erp_cust_az12
        PRINT '>>>>> INSERTING DATA INTO: silver.erp_cust_az12'
        INSERT
        INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' --NAS is a prefix to the id that is there for some reason and should be removed to allow joins
                    THEN SUBSTRING(cid, 4, LEN(cid) - 3)
                ELSE cid
            END cid,
            CASE
                WHEN bdate > GETDATE()
                    THEN NULL
                ELSE bdate
            END bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE')
                    THEN 'F'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')
                    THEN 'M'
                ELSE 'N/A'
            END gen
        FROM bronze.erp_cust_az12

        TRUNCATE TABLE silver.erp_loc_a101
        PRINT '>>>>> INSERTING DATA INTO: silver.erp_loc_a101'
        INSERT
        INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') cid,  --the cid here has a - between the AW and the number part, we dont want that
            CASE
                WHEN TRIM(cntry) = 'DE'           THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = 'UK'           THEN 'United Kingdom'
                WHEN cntry = '' OR cntry IS NULL  THEN 'N/A'
                ELSE cntry
            END                   cntry --Normalizing and handling missing country codes
        FROM bronze.erp_loc_a101

        TRUNCATE TABLE silver.erp_px_cat_g1v2
        PRINT '>>>>> INSERTING DATA INTO: silver.erp_px_cat_g1v2'
        INSERT
        INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT --perfect data quality, so no changes necessary
               id,
               cat,
               subcat,
               maintenance
        FROM bronze.erp_px_cat_g1v2
    END TRY
    BEGIN CATCH --ERROR CATCHER
        PRINT '======================================'
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE()
        PRINT 'ERROR MESSAGE: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
        PRINT '======================================'
    END CATCH
END