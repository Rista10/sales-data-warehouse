-- silver.crm_cust_info
-- checking for duplicate cst_id
SELECT cst_id, count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1

-- checking for extra spaces present
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data Standardization and Consistency
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

-- silver.crm_prd_info
SELECT prd_id , count(*) 
FROM silver.crm_prd_info
GROUP by prd_id
HAVING count(*)>1

-- check for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- check for null or negative numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT * 
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt

-- silver.crm_sales_details
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details

-- check for invalid date
SELECT sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt)<8

SELECT sls_order_dt, sls_ship_dt, sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt

SELECT DISTINCT
sls_sales, 
sls_quantity,
sls_price,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
     THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
     ELSE sls_price
END as sls_price,
sls_quantity,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END as sls_sales
FROM silver.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <=0

-- silver.erp_cust_az12
-- identify out-of-range dates
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- data standardization and consistency
SELECT DISTINCT 
    gen,
    CASE
        WHEN UPPER(REPLACE(REPLACE(LTRIM(RTRIM(gen)), CHAR(13), ''), CHAR(10), '')) 
             IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(REPLACE(REPLACE(LTRIM(RTRIM(gen)), CHAR(13), ''), CHAR(10), '')) 
             IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM silver.erp_cust_az12;

-- silver.erp_loc_a101
SELECT 
cid,
cntry
FROM silver.erp_loc_a101

SELECT REPLACE(cid,'-','') as cid
FROM silver.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- Data standardization and consistency
SELECT DISTINCT cntry,
CASE WHEN REPLACE(TRIM(cntry), CHAR(13),'') = 'DE' THEN 'Germany'
     WHEN REPLACE(TRIM(cntry), CHAR(13),'') IN ('US','USA') THEN 'United Sates'
     WHEN REPLACE(TRIM(cntry), CHAR(13),'') = '' OR cntry IS NULL THEN 'n'
     ELSE REPLACE(TRIM(cntry), CHAR(13),'')
END AS country
FROM silver.erp_loc_a101
ORDER BY cntry

-- silver.erp_px_cat_g1v2
SELECT 
id,
cat,
subcat,
maintenance
FROM silver.erp_px_cat_g1v2

SELECT 
id,
cat,
subcat,
CASE WHEN maintenance != REPLACE(TRIM(maintenance),CHAR(13),'') THEN  REPLACE(TRIM(maintenance),CHAR(13),'')
     ELSE maintenance
END AS maintenance
FROM silver.erp_px_cat_g1v2
WHERE cat!= TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != REPLACE(TRIM(maintenance),CHAR(13),'')

-- data standardization and consistency 
SELECT DISTINCT 
id,
cat,
subcat,
maintenance
FROM silver.erp_px_cat_g1v2
