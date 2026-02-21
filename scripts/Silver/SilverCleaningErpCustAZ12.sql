--Identify dates out-of-range or impossible
SELECT DISTINCT
    bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > GETDATE()

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