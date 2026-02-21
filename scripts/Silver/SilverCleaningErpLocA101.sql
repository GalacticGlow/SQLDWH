--Data standartization and consistency
SELECT DISTINCT
    cntry
FROM bronze.erp_loc_a101

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