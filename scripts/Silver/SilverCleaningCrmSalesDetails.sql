--Check for invalid dates
SELECT
    NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0       --date validation to make sure the int number can be converted to a date
   OR LEN(sls_order_dt) != 8
   OR sls_order_dt > 20500101 --biggest date realistically possible in the dataset
   OR sls_order_dt < 19000101
--smallest

--Check for invalid date orders (order date has to be smaller than ship date)
SELECT
    *
FROM bronze.crm_sales_details
WHERE sls_ship_dt < sls_order_dt
   OR sls_order_dt > sls_due_dt

--Business rule: Sales = Quantity * Price, amd Sales that are <= 0 and NULL arent allowed
--Check if this is true for all columns
SELECT DISTINCT
    sls_price,
    sls_quantity,
    sls_sales
FROM bronze.crm_sales_details
WHERE sls_price * sls_quantity != sls_sales
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price
--If you get problems here you usually talk to the source system owners to see what to do about it
--Usually they help direct you to the correct solution for the business
/*
 The given rules in this case:
 - If Sales <= 0 or NULL, derive it using quantity or price
 - If Price is 0 or Null, calculate using Sales and quantity
 - If Price < 0, just convert it to a positive value
 */
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