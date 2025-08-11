CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    BEGIN TRY
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        DECLARE @start_time DATETIME, @end_time DATETIME;
        DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

        SET @batch_start_time = GETDATE();

        ---------------- silver.crm_cust_info ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT 'Time to truncate silver.crm_cust_info: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, cst_gndr, cst_marital_status, cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
        ) t
        WHERE flag_last = 1;
        PRINT 'Time to insert silver.crm_cust_info: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        ---------------- silver.crm_prd_info ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT 'Time to truncate silver.crm_prd_info: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
        FROM bronze.crm_prd_info;
        PRINT 'Time to insert silver.crm_prd_info: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        ---------------- silver.crm_sales_details ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT 'Time to truncate silver.crm_sales_details: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN LEN(sls_order_dt) < 8 OR sls_order_dt <= 0 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE),
            CAST(CAST(sls_due_dt AS VARCHAR) AS DATE),
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 
                    THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END,
            sls_quantity,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END
        FROM bronze.crm_sales_details;
        PRINT 'Time to insert silver.crm_sales_details: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        ---------------- silver.erp_cust_az12 ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT 'Time to truncate silver.erp_cust_az12: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid, bdate, gen
        )
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END,
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END,
            CASE 
                WHEN UPPER(REPLACE(REPLACE(LTRIM(RTRIM(gen)), CHAR(13), ''), CHAR(10), '')) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(REPLACE(REPLACE(LTRIM(RTRIM(gen)), CHAR(13), ''), CHAR(10), '')) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;
        PRINT 'Time to insert silver.erp_cust_az12: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        ---------------- silver.erp_loc_a101 ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT 'Time to truncate silver.erp_loc_a101: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid, cntry
        )
        SELECT 
            REPLACE(cid, '-', ''),
            CASE 
                WHEN REPLACE(TRIM(cntry), CHAR(13), '') = 'DE' THEN 'Germany'
                WHEN REPLACE(TRIM(cntry), CHAR(13), '') IN ('US', 'USA') THEN 'United States'
                WHEN REPLACE(TRIM(cntry), CHAR(13), '') = '' OR cntry IS NULL THEN 'n'
                ELSE REPLACE(TRIM(cntry), CHAR(13), '')
            END
        FROM bronze.erp_loc_a101;
        PRINT 'Time to insert silver.erp_loc_a101: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        ---------------- silver.erp_px_cat_g1v2 ----------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT 'Time to truncate silver.erp_px_cat_g1v2: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id, cat, subcat, maintenance
        )
        SELECT 
            id,
            cat,
            subcat,
            CASE 
                WHEN maintenance != REPLACE(TRIM(maintenance), CHAR(13), '') 
                    THEN REPLACE(TRIM(maintenance), CHAR(13), '')
                ELSE maintenance
            END
        FROM bronze.erp_px_cat_g1v2;
        PRINT 'Time to insert silver.erp_px_cat_g1v2: ' 
            + CAST(DATEDIFF(second, @start_time, GETDATE()) AS NVARCHAR) + ' seconds';

        ---------------- Total Batch ----------------
        SET @batch_end_time = GETDATE();
        PRINT 'Total Batch Time: ' 
            + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

    END TRY
    BEGIN CATCH
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
    END CATCH
END;
