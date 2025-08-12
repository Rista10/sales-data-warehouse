CREATE VIEW gold.dim_customers AS
SELECT 
 ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
ca.cst_id AS customer_id,
ca.cst_key AS customer_number,
ca.cst_firstname AS customer_firstname,
ca.cst_lastname AS customer_lastname,
ca.cst_marital_status AS customer_marital_status,
ci.bdate AS customer_birthdate,
CASE WHEN cst_gndr != 'n/a' THEN cst_gndr
     ELSE COALESCE('n/a',ci.gen) 
END AS customer_gender,
li.cntry AS customer_country,
ca.cst_create_date AS customer_create_date
FROM  [silver].[crm_cust_info] as ca
LEFT JOIN  [silver].[erp_cust_az12] as ci ON ca.cst_key = ci.cid
LEFT JOIN  [silver].[erp_loc_a101] as li ON ca.cst_key = li.cid

CREATE VIEW gold.dim_products AS
SELECT 
 ROW_NUMBER() OVER (ORDER BY pr.prd_key,pr.prd_start_dt) AS product_key,
 pr.prd_id AS product_id,
 pr.prd_key AS product_number,
 pr.prd_nm AS product_name,
 pr.cat_id AS category_id,
 px.cat AS category,
 px.subcat AS subcategory,
 px.maintenance,
 pr.prd_cost AS product_cost,
 pr.prd_line AS product_line,
 pr.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pr
LEFT JOIN silver.erp_px_cat_g1v2 AS px ON  pr.cat_id = px.id
WHERE  pr.prd_end_dt IS NULL

CREATE VIEW gold.fact_sales AS 
SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_key
