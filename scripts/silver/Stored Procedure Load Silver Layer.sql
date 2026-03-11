

/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.Laod_silver_layer;
===============================================================================
*/


CREATE OR ALTER PROCEDURE  silver.Laod_silver_layer
AS
BEGIN
DECLARE @start_time DATETIME,@end_time DATETIME,@batchstart DATETIME,@batchend DATETIME

   BEGIN TRY
SET @batchstart = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';


        --------------Insert into (Clean and Load Silver Layer)silver.crm_cust_info 
        SET @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data Into: silver.crm_cust_info';


    INSERT INTO silver.crm_cust_info(
           cst_id
          ,cst_key
          ,cst_firstname
          ,cst_lastname
          ,cst_marital_status
          ,cst_gndr
          ,cst_create_date
          )
    SELECT t.cst_id, 
           t.cst_key, 
           TRIM(t.cst_firstname) cst_firstname,
           TRIM(t.cst_lastname) cst_lastname, 
          CASE
          WHEN  UPPER(TRIM(t.cst_marital_status))='M' THEN 'Married'
          WHEN UPPER(TRIM(t.cst_marital_status))='F' THEN 'Single' ELSE 'N/A' END [cst_marital_status],
          CASE
          WHEN UPPER(TRIM(t.cst_gndr))='F' THEN 'Female'
          WHEN UPPER(TRIM(t.cst_gndr))='M' THEN 'Male'
          ELSE 'N/A' END  cst_gndr,
          t.cst_create_date
    FROM(SELECT *, ROW_NUMBER() OVER (PARTITION BY CCI.cst_id ORDER BY CCI.cst_create_date DESC) [Rank]
         FROM bronze.crm_cust_info CCI) t
    WHERE t.Rank=1 AND t.cst_id IS NOT NULL;

    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

    --------------Insert into (Clean and Load Silver Layer)silver.crm_prd_info

     SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key,prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    
	    SELECT 
	    CPI.prd_id,
	    TRIM(SUBSTRING(REPLACE(CPI.prd_key,'-','_'),1,5)) cat_id,
	    TRIM(SUBSTRING(CPI.prd_key,7,LEN(CPI.prd_key))) prd_key,
	    CPI.prd_nm,
	    ISNULL(CPI.prd_cost,0) prd_cost,
	    CASE
	    WHEN  UPPER(trim(CPI.prd_line))='M' THEN'Mountain'
	    WHEN  UPPER(trim(CPI.prd_line))='R' THEN'Road'
	    WHEN  UPPER(trim(CPI.prd_line))='S' THEN'Other Sales'
	    WHEN  UPPER(trim(CPI.prd_line))='T' THEN'Touring'
	    ELSE 'N/A'
	    END
	    prd_line,
	    CAST(CPI.prd_start_dt AS DATE) prd_start_dt,
	    CAST(LEAD(CPI.prd_start_dt) OVER(PARTITION BY CPI.prd_key ORDER BY CPI.prd_start_dt)-1 AS DATE) prd_end_dt
	    FROM bronze.crm_prd_info CPI 

          SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


	    --ORDER BY  TRIM(SUBSTRING(REPLACE(CPI.prd_key,'-','_'),1,5));

    --SELECT * FROM bronze.erp_px_cat_g1v2 EPCG ORDER BY EPCG.ID

    --SELECT * FROM bronze.crm_sales_details CSD WHERE CSD.sls_prd_key='WB-H098' ORDER BY CSD.sls_prd_key 





    --------------Insert into (Clean and Load Silver Layer)silver.crm_sales_details

    SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    SELECT 
    CSD.sls_ord_num,
    CSD.sls_prd_key,
    CSD.sls_cust_id,

    CASE WHEN CSD.sls_order_dt <=0 OR LEN(CSD.sls_order_dt)!=8  THEN NULL
    ELSE
    CAST(CAST(CSD.sls_order_dt AS VARCHAR) AS DATE)
    END sls_order_dt,

    CASE WHEN CSD.sls_ship_dt <=0 OR LEN(CSD.sls_ship_dt)!=8  THEN NULL
    ELSE
    CAST(CAST(CSD.sls_ship_dt AS VARCHAR) AS DATE)
    END sls_ship_dt,

    CASE WHEN CSD.sls_due_dt <=0 OR LEN(CSD.sls_due_dt)!=8  THEN NULL
    ELSE
    CAST(CAST(CSD.sls_due_dt AS VARCHAR) AS DATE)
    END sls_due_dt,

    CASE
     WHEN CSD.sls_sales IS NULL 
     OR CSD.sls_sales <=0 
     THEN ABS(CSD.sls_quantity)*ABS(CSD.sls_price) ELSE ABS(CSD.sls_sales)  END sls_sales,

    CSD.sls_quantity,

    CASE 
     WHEN CSD.sls_price IS NULL 
     OR CSD.sls_price <=0 
     OR ABS(CSD.sls_price) != ABS(CSD.sls_sales) /NULLIF(ABS( CSD.sls_quantity),0)
     THEN ABS( CSD.sls_sales) / NULLIF(ABS(CSD.sls_quantity),0) ELSE ABS( CSD.sls_price) END sls_price

      FROM bronze.crm_sales_details CSD
       WHERE NOT EXISTS(
       SELECT 1 FROM silver.crm_sales_details CSD2 
       WHERE  CSD2.sls_ord_num =CSD.sls_ord_num
       AND CSD2.sls_prd_key=CSD.sls_prd_key
       )


    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

   
       ----------------------------silver.erp_cust_az12
       
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

       SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';


       INSERT INTO silver.erp_cust_az12(CID, BDATE, gen)
 

       SELECT
    CASE WHEN ECA.CID LIKE 'NAS%' THEN SUBSTRING(ECA.CID, 4, LEN(ECA.CID)) ELSE ECA.CID END cid,
    CASE WHEN ECA.BDATE>GETDATE() THEN NULL ELSE ECA.BDATE END BDATE,
    CASE WHEN  ECA.gen   IN  ('F','Female') THEN 'Female' 
    WHEN  ECA.gen   IN  ('M','Male') THEN 'Male' 
    ELSE 'N/A' END gen
    FROM bronze.erp_cust_az12 ECA
    WHERE NOT EXISTS(SELECT 1 FROM silver.erp_cust_az12 ECA2 WHERE ECA.CID=ECA.CID )

     SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';



    ------------------------------------------------- INSERT INTO silver.erp_loc_a101

     SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101(CID, CNTRY)


    SELECT REPLACE(ELA.CID,'-','') cid,
    CASE
         WHEN UPPER(TRIM(ELA.CNTRY))IN ('US','USA') THEN 'United States' 
         WHEN UPPER(TRIM(ELA.CNTRY))='DE' THEN 'Germany' 
         WHEN UPPER(TRIM(ELA.CNTRY))=' ' THEN 'N/A' 
         WHEN ELA.CNTRY IS NULL THEN 'N/A'
         ELSE TRIM(ELA.CNTRY)

    END CNTRY FROM bronze.erp_loc_a101 ELA
    WHERE NOT EXISTS(SELECT 1 FROM silver.erp_loc_a101 ELA WHERE ELA.CID=ELA.CID )


    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

    ------------------------------------------------- INSERT INTO silver.erp_px_cat_g1v2 
    SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2(ID, CAT, SUBCAT, MAINTENANCE)

    SELECT EPCG.ID,EPCG.CAT,EPCG.SUBCAT,EPCG.MAINTENANCE FROM bronze.erp_px_cat_g1v2 EPCG
    WHERE NOT EXISTS(SELECT * FROM silver.erp_px_cat_g1v2 EPCG2 WHERE EPCG2.ID=EPCG.ID )
    SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batchend = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batchstart, @batchend) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

    END TRY
    BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
  END


  --EXEC silver.Laod_silver_layer
