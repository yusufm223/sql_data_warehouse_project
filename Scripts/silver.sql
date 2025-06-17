IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
   DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gender NVARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
--------------------------------------------------

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
   DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info(
	prd_id       INT,
	prd_key      NVARCHAR(50),
	cat_id      NVARCHAR(50),
	prd_nm       NVARCHAR(50),
	prd_cost     INT,
	prd_line     NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt   DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
------------------------------------------------

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
   DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
	sls_ord_num    NVARCHAR(50),
	sls_prd_key    NVARCHAR(50),
	sls_cust_id    INT,
	sls_order_dt   DATE,
	sls_ship_dt    DATE,
	sls_due_dt     DATE,
	sls_sales      INT,
	sls_quantity   INT,
	sls_price      INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-----------------------------------------------------

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
   DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid   NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

--------------------------------------------------------

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
   DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
    cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
--------------------------------------------------------

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
   DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id          NVARCHAR(50),
	cat         NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);






----------------------------------------------------------------------------
--------------------------DATA CLEANING-------------------------------------
----------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN

PRINT '======================================================'
PRINT 'Loading Silver Layer'
PRINT '======================================================'

PRINT '------------------------------------------------------'
PRINT 'Loading CRM Tables'
PRINT '------------------------------------------------------'


	PRINT '>>> Truncating Table : silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;

	PRINT '>>> Inserting Into Table: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(

	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gender,
	cst_create_date)

	SELECT 
	   cst_id,
	   cst_key,
	   TRIM(cst_firstname) AS cst_firstname,
	   TRIM(cst_lastname) AS cst_lastname,
	   CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status)) = 'F' THEN  'Single'
			ELSE 'n/a'
	   END AS cst_marital_status,
	   CASE WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
			ELSE 'n/a' 
	   END	AS cst_gender,
	   cst_create_date
	FROM (
			SELECT *,
			row_number() over(partition by cst_id order by cst_create_date desc) as flag
			FROM bronze.crm_cust_info
			WHERE cst_id is not null) as t
	WHERE FLAG =1;

	-------------------------------------------------------------------------------

	PRINT '>>> Truncating Table : silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;

	PRINT '>>> Insert Into Table : silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
		prd_id,      
		prd_key,      
		cat_id,      
		prd_nm,       
		prd_cost,     
		prd_line,
		prd_start_dt, 
		prd_end_dt
	)
	SELECT TOP (1000) 
				prd_id,
				SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key,
				REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
				prd_nm,
				ISNULL(prd_cost,0) AS prd_cost,
				CASE WHEN prd_line = 'M' THEN 'Mountain'
					 WHEN prd_line = 'R' THEN 'Road'
					 WHEN prd_line = 'S' THEN 'Other Sales'
					 WHEN prd_line = 'T' THEN 'Touring'
					 ELSE 'n/a'
				END AS prd_line,
				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
	  FROM  bronze.crm_prd_info;

	-------------------------------------------------------------------------
	PRINT '>>> Truncating Table : silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;

	PRINT '>>> Insert Into Table : silver.crm_sales_details'; 

	INSERT INTO silver.crm_sales_details(
		   sls_ord_num
		  ,sls_prd_key
		  ,sls_cust_id
		  ,sls_order_dt
		  ,sls_ship_dt
		  ,sls_due_dt
		  ,sls_sales
		  ,sls_quantity
		  ,sls_price
    		 )

	SELECT sls_ord_num
		  ,sls_prd_key
		  ,sls_cust_id
		  ,CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ElSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		   END AS sls_order_dt	
		  ,CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt as VARCHAR) AS DATE) 
		   END AS sls_ship_dt
		  ,CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		   END AS sls_due_dt
		  ,CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
		   END AS sls_sales
		  ,sls_quantity
		  ,CASE WHEN sls_price IS NULL OR sls_price <= 0 
				THEN sls_sales / NULLIF (sls_quantity,0)
				ELSE sls_price
		   END AS sls_price
      
	  FROM bronze.crm_sales_details;

	-------------------------------------------------------------------------

PRINT '---------------------------------------------------------------'
PRINT 'Loading ERP Tables'
PRINT '---------------------------------------------------------------'

	PRINT '>>> Truncating Table : silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;

	PRINT '>>> Insert Into Table : silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)

	select 
		 CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			   ELSE cid
		 END AS cid,
  
		 CASE WHEN bdate > GETDATE() THEN NULL
			  ELSE bdate
		 END AS bdate,

		 CASE WHEN UPPER(TRIM(gen))  IN('F','FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN('M','MALE') THEN 'Male'
				 ELSE 'n/a'
		 END AS gen
	FROM bronze.erp_cust_az12; 

	------------------------------------------------------------------------
	PRINT '>>> Truncating Table : silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;

	PRINT '>>> Insert Into Table : silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
	)
	SELECT 
		REPLACE(cid,'-','') AS cid,

		CASE WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
			 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry)	= ' ' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101;

	---------------------------------------------------------------------
	PRINT '>>> Truncating Table : silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	PRINT '>>> Insert Into Table : silver.erp_px_cat_g1v2'
	INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
	SELECT * 
	FROM bronze.erp_px_cat_g1v2;
	------------------------------------------------------------------------
END;



exec silver.load_silver;



















