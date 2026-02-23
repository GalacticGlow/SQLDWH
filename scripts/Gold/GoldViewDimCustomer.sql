--Here in the gold layer we apply business logic to the data and create a business-ready data model
--No tables - only views, as the business uses them the most often

--Here we have 2 sources of customer gender from the crm and erp systems
--We were told that the crm is the master system for gender and should be trusted more if there are conflicts in the data
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'N/A')
    END gender
FROM silver.crm_cust_info      ci
LEFT JOIN silver.erp_cust_az12 ca
          ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101  la
          ON ci.cst_key = la.cid
ORDER BY 1, 2

--Dimension table
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, --surrogate key - a key that you artificially create to reference each row in the table
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_material_status AS marital_status,
    ci.cst_create_date AS create_date,
    ca.bdate AS birthdate,
    CASE
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'N/A')
    END gender
FROM silver.crm_cust_info      ci
LEFT JOIN silver.erp_cust_az12 ca
          ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101  la
          ON ci.cst_key = la.cid

SELECT * FROM gold.dim_customers