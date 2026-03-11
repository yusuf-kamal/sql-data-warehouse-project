/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================

*/

DROP TABLE IF EXISTS silver.crm_cust_info
BEGIN
PRINT 'silver.crm_cust_info Table succefuly Created'
CREATE  TABLE silver.crm_cust_info(
cst_id INT ,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_Create_date DATETIME2 DEFAULT GETDATE()
);
END
GO

DROP TABLE IF EXISTS silver.crm_prd_info
BEGIN
PRINT 'silver.crm_prd_info Table succefuly Created'
CREATE TABLE silver.crm_prd_info(
prd_id INT ,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_Create_date DATETIME2 DEFAULT GETDATE()

);
END
GO



DROP TABLE IF EXISTS silver.crm_sales_details
BEGIN
 PRINT 'silver.crm_sales_details Table succefuly Created'
CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR(50) ,
sls_prd_key NVARCHAR(50),
sls_cust_id INT ,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_Create_date DATETIME2 DEFAULT GETDATE()

);
END
GO


DROP TABLE IF EXISTS silver.erp_cust_az12
BEGIN
PRINT 'silver.erp_cust_az12 Table succefuly Created'
CREATE TABLE silver.erp_cust_az12(
CID NVARCHAR(50) ,
BDATE DATE,
gen NVARCHAR(50),
dwh_Create_date DATETIME2 DEFAULT GETDATE()

);
END
GO



DROP TABLE IF EXISTS silver.erp_loc_a101
BEGIN
PRINT 'silver.erp_loc_a101 Table succefuly Created'
CREATE TABLE silver.erp_loc_a101(
CID NVARCHAR(50) ,
CNTRY NVARCHAR(50),
dwh_Create_date DATETIME2 DEFAULT GETDATE()

);
END
GO

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2
BEGIN
PRINT ' silver.erp_px_cat_g1v2 Table succefuly Created'
CREATE TABLE silver.erp_px_cat_g1v2(
ID NVARCHAR(50) ,
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_Create_date DATETIME2 DEFAULT GETDATE()

);
END
GO
