--This is the place where we check for Nulls in the Primary keys
--Expectation: No result

SELECT cst_id,
       COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
    OR cst_id IS NULL

--Resolving the duplicate and NULL pk-s
--The next query has 3 duplicate values, one of which is more complete than the other two, and more recent, so we choose it to go in the final result
SELECT *
FROM (SELECT *,
             ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
      FROM bronze.crm_cust_info) t
WHERE flag_last = 1

--Checking for unwanted trailing spaces in the string values
--Expectation: No result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--Data standardization and consistency: checking to make sure the gender and marital statuses are consistent
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info


--FINAL INSERT QUERY FOR cst_cust_info
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