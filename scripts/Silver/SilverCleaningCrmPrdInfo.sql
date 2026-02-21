--Checking for NULLS or Negative Numbers
--Expectation: No results
SELECT
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

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