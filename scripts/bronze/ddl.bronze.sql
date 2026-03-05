/*
DDl Script : Create Bronze Layer

Script Purpose :
This Script Create in The Bronz Schema and Droping Existing Table If Exist


*/


DROP TABLE IF EXISTS bronze.crm_cust_info
BEGIN
PRINT 'bronze.crm_cust_info Table succefuly Created'
CREATE  TABLE bronze.crm_cust_info(
cst_id INT ,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);
END
GO

DROP TABLE IF EXISTS bronze.crm_prd_info
BEGIN
PRINT 'bronze.crm_prd_info Table succefuly Created'
CREATE TABLE bronze.crm_prd_info(
prd_id INT ,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME,
);
END
GO



DROP TABLE IF EXISTS bronze.crm_sales_details
BEGIN
 PRINT 'bronze.crm_sales_details Table succefuly Created'
CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50) ,
sls_prd_key NVARCHAR(50),
sls_cust_id INT ,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt int,
sls_sales INT,
sls_quantity INT,
sls_price INT
);
END
GO


DROP TABLE IF EXISTS bronze.erp_cust_az12
BEGIN
PRINT 'bronze.erp_cust_az12 Table succefuly Created'
CREATE TABLE bronze.erp_cust_az12(
CID NVARCHAR(50) ,
BDATE DATE,
gen NVARCHAR(50)
);
END
GO



DROP TABLE IF EXISTS bronze.erp_loc_a101
BEGIN
PRINT 'bronze.erp_loc_a101 Table succefuly Created'
CREATE TABLE bronze.erp_loc_a101(
CID NVARCHAR(50) ,
CNTRY NVARCHAR(50)
);
END
GO

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2
BEGIN
PRINT ' bronze.erp_px_cat_g1v2 Table succefuly Created'
CREATE TABLE bronze.erp_px_cat_g1v2(
ID NVARCHAR(50) ,
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50)
);
END
GO
