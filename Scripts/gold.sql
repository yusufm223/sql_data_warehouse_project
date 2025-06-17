-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID ('gold.dim_customers', 'V') IS NOT NULL
   DROP VIEW gold.dim_customers;

Go

CREATE VIEW gold.dim_customers AS 
SELECT 
            ROW_NUMBER() Over(ORDER BY ci.cst_id) AS customer_key
		   ,ci.cst_id AS customer_id
		  ,ci.cst_key AS customer_number
		  ,ci.cst_firstname AS first_name
		  ,ci.cst_lastname AS last_name,
		  la.cntry AS country
		  ,ci.cst_marital_status AS marital_status
		  ,CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender
		      ELSE COALESCE(ca.gen, 'n/a')
           END AS gender,
		   ca.bdate AS birthdate
		  ,ci.cst_create_date AS create_date
 FROM silver.crm_cust_info ci
 LEFT JOIN silver.erp_cust_az12 ca 
 ON ca.cid = ci.cst_key
 LEFT JOIN silver.erp_loc_a101 la
 ON ci.cst_key =la.cid;


 -- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

 IF OBJECT_ID ('gold.dim_products', 'V') IS NOT NULL
   DROP VIEW gold.dim_products;

Go
 
CREATE VIEW gold.dim_products AS
SELECT
       ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key
      ,pn.prd_id AS product_id
      ,pn.prd_key AS product_number
	  ,pn.prd_nm AS product_name
      ,pn.cat_id AS category_id
      ,pc.cat AS category
	  ,pc.subcat AS subcategory
	  ,pc.maintenance
      ,pn.prd_cost AS cost
      ,pn.prd_line AS product_line
      ,pn.prd_start_dt AS start_date
	  
  FROM silver.crm_prd_info pn
  LEFT JOIN silver.erp_px_cat_g1v2 pc
  ON pn.cat_id = pc.id 
  WHERE prd_end_dt IS NULL -- filter out all historical records
  
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

 IF OBJECT_ID ('gold.fact_sales', 'V') IS NOT NULL
   DROP VIEW gold.fact_sales;

Go

CREATE VIEW gold.fact_sales AS 
SELECT sls_ord_num AS order_number
      ,pr.product_key  
	  ,cu.customer_key
      ,sls_order_dt AS order_date
      ,sls_ship_dt AS ship_date
      ,sls_due_dt AS due_date
      ,sls_sales AS sales_amount
	  ,sls_quantity AS quantity
      ,sls_price AS price
  FROM silver.crm_sales_details sd
  LEFT JOIN gold.dim_products pr
  ON sd.sls_prd_key = pr.product_number
  LEFT JOIN gold.dim_customers cu
  ON sd.sls_cust_id = cu.customer_id;