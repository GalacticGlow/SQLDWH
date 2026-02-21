--Checking for unwanted spaces
SELECT
    *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance)

--Data standardization and consistency
SELECT DISTINCT
    cat
FROM bronze.erp_px_cat_g1v2

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