--This is central fact table, and we connect all the dimension tables to it using the surrogate keys
--So we dont need sls_prd_key and sls_cust_id, as they come from the source system

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key, --we put the surrogate keys in here to make joining with the other tables possible
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales,
    sd.sls_price AS price,
    sd.sls_quantity AS quantity
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
--the best schema to use when building fact tables is like this:
--dimension keys grouped up to the left, dates in the center and measures (numbers usually) on the right
--makes code more readable

--Data model integrity check: expecting no result
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL