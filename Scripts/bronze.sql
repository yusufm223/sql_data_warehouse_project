-- Create Database 'DataWarehouse'

USE master; 

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gender NVARCHAR(50),
	cst_create_date DATE 
);
--------------------------------------------------

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
	prd_id       INT,
	prd_key      NVARCHAR(50),
	prd_nm       NVARCHAR(50),
	prd_cost     INT,
	prd_line     NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt   DATETIME
);
------------------------------------------------

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
   DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
sls_ord_num    NVARCHAR(50),
sls_prd_key    NVARCHAR(50),
sls_cust_id    INT,
sls_order_dt   INT,
sls_ship_dt    INT,
sls_due_dt     INT,
sls_sales      INT,
sls_quantity   INT,
sls_price      INT
);

-----------------------------------------------------

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
   DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid   NVARCHAR(50),
	cntry NVARCHAR(50)
);

--------------------------------------------------------

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
   DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
    cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);
--------------------------------------------------------

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
   DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id          NVARCHAR(50),
	cat         NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance NVARCHAR(50)
);



------------------------------------------------------------------
------------------------BULK LOADING DATA-------------------------
------------------------------------------------------------------
EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN

	PRINT '==========================================================';
	PRINT 'Loading Bronze Layer'
	PRINT '==========================================================';

	PRINT '----------------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '----------------------------------------------------------';


	PRINT'>>> Truncating Table : bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	PRINT '>>> Inserting Data Into : bronze.crm_cust_info';
	BULK INSERT bronze.crm_cust_info
	FROM "C:\Users\yusuf\Downloads\source_crm\cust_info.csv"
	WITH(
	   FIRSTROW = 2,
	   FIELDTERMINATOR = ',',
	   TABLOCK
	);


	PRINT '>>> Truncating Table : bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info

	PRINT '>>> Inserting Data Into : bronze.crm_prd_info';
	BULK INSERT bronze.crm_prd_info
	FROM "C:\Users\yusuf\Downloads\source_crm\prd_info.csv"
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
	);
 

	PRINT '>>> Truncating Table : bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	PRINT'>>> Inserting Data Into : bronze.crm_sales_details'
	BULK INSERT bronze.crm_sales_details
	FROM "C:\Users\yusuf\Downloads\source_crm\sales_details.csv"
	WITH(
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	);

	PRINT '>>> Truncating Table : bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT'>>> Inserting Table : bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM "C:\Users\yusuf\Downloads\Data with Baraa 2025\datasets\source_erp\LOC_A101.csv"
	WITH(
	   FIRSTROW = 2,
	   FIELDTERMINATOR = ',',
	   TABLOCK
	);

	PRINT '>>> Truncating Table : bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	PRINT '>>> Inserting Table : bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM "C:\Users\yusuf\Downloads\Data with Baraa 2025\datasets\source_erp\CUST_AZ12.csv"
	WITH(
	   FIRSTROW = 2,
	   FIELDTERMINATOR = ',',
	   TABLOCK
	);

	PRINT '>>> Truncating Table : bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2
	PRINT '>>> Inserting Table : bronze.erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM "C:\Users\yusuf\Downloads\Data with Baraa 2025\datasets\source_erp\PX_CAT_G1V2.csv"
	WITH(
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	);

END

















































