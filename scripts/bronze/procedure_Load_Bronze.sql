USE [DataWarehouse]
GO
/****** Object:  StoredProcedure [bronze].[bronze.Load_Bronze_Layer]    Script Date: 06/03/2026 12:53:29 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/*
Stored Procedure : Load Bronze Layer

Script Purpose :
This Is Stored Procedure  Laods Data  Into The Bronze  Schema  From External CSV FIle
it Performing The Following Action :
	- Truncate The Bronze Tables Before Loading Data
	- Use The Bulk Insert  Command To load data From csv File

Parameters :
	None 

Usage Example:
	EXEC bronze.Load_Bronze_Layer
*/

CREATE OR ALTER   PROCEDURE [bronze].[Load_Bronze_Layer] 
AS
BEGIN
DECLARE @starttime DATETIME,@endtime DATETIME
 BEGIN TRY
  PRINT'============================='
   PRINT 'Loading Bronze Layer'
  PRINT'============================='

  
  
  PRINT'-----------------------------'
  PRINT' Loading CRM Tables'
  PRINT'-----------------------------'
   
   SET @starttime=GETDATE();
   PRINT'TRUNCATE TABLE  bronze.crm_cust_info successfully'
  TRUNCATE TABLE  bronze.crm_cust_info
  PRINT'BULK INSERT bronze.crm_cust_info successfully'
	BULK INSERT bronze.crm_cust_info
	FROM 'D:\datasets\source_crm\cust_info.csv'
	WITH(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);

	PRINT'TRUNCATE TABLE [bronze].[crm_prd_info] successfully'
	TRUNCATE TABLE [bronze].[crm_prd_info]
	PRINT'BULK INSERT [bronze].[crm_prd_info]'
	BULK INSERT [bronze].[crm_prd_info]
	FROM 'D:\datasets\source_crm\prd_info.csv'
	WITH(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);

	PRINT'TRUNCATE TABLE [bronze].[crm_sales_details] successfully'
	TRUNCATE TABLE [bronze].[crm_sales_details]
	PRINT'BULK INSERT [bronze].[crm_sales_details] successfully'
	BULK INSERT [bronze].[crm_sales_details]
	FROM 'D:\datasets\source_crm\sales_details.csv'
	WITH(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);

	
  PRINT'-----------------------------'
  PRINT' Loading ERP Tables'
  PRINT'-----------------------------'

    PRINT 'TRUNCATE TABLE [bronze].[erp_cust_az12] successfully'
	TRUNCATE TABLE [bronze].[erp_cust_az12]
    PRINT'BULK INSERT [bronze].[erp_cust_az12] successfully'
	BULK INSERT [bronze].[erp_cust_az12]
	FROM 'D:\datasets\source_erp\cust_az12.csv'
	WITH(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);


	PRINT'TRUNCATE TABLE [bronze].[erp_loc_a101] successfully'
	TRUNCATE TABLE [bronze].[erp_loc_a101]
	PRINT'BULK INSERT [bronze].[erp_loc_a101] successfully'
	BULK INSERT [bronze].[erp_loc_a101]
	FROM 'D:\datasets\source_erp\loc_a101.csv'
	WITH(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);

	PRINT'TRUNCATE TABLE [bronze].[erp_px_cat_g1v2] successfully'
	TRUNCATE TABLE [bronze].[erp_px_cat_g1v2]
	PRINT'BULK INSERT [bronze].[erp_px_cat_g1v2] successfully'
	BULK INSERT [bronze].[erp_px_cat_g1v2]
	FROM 'D:\datasets\source_erp\px_cat_g1v2.csv'
	WITH(FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);

	SET @endtime=GETDATE();
	PRINT 'Excuted Time by Second' +' ' + CAST(DATEDIFF(SECOND,@starttime,@endtime) AS NVARCHAR(50))
 END TRY
 BEGIN CATCH
PRINT 'Error Occured During Loading Bronze Layer '
 PRINT 'Error Message' + ERROR_MESSAGE()
 PRINT 'Error Number' + CAST( ERROR_NUMBER() AS NVARCHAR(MAX))
 PRINT 'Error State' + CAST( ERROR_STATE() AS NVARCHAR(50))

 END CATCH

END

