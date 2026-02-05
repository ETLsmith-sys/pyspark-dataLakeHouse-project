from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import * 

spark = (
    SparkSession.builder
    .appName("GoldLayer")
    .enableHiveSupport()
    .getOrCreate()
)




class GoldLayer:
    """ 
    GoldLayer class ---------------- 
    This class builds the Gold Layer by reading data from the Silver Layer,
    creating Dimension tables (Customer, Product) and a Fact table (Sales),
    and saving them into the Gold Layer for business analysis. 
    """
    def __init__(self):
        self.base_path = 'workspace.silver.'
        self.tabelsLst =[
        'source_crm_cust_info',
        'source_crm_prd_info',
        'source_crm_sales_details',
        'source_erp_cust_az12',
        'source_erp_loc_a101',
        'source_erp_px_cat_g1v2']
        
        self.dfs = self.readFromSilverLayer()
        self.DimensionCutomer = self.createDimensionCutomer() 
        self.DimensionProduct = self.createDimensionProduct() 
        self.FactSales = self.createFactSales()
        
    def readFromSilverLayer(self):
        """Reads all required tables from the Silver Layer into Spark DataFrames."""
        dfs = {}
        for table in self.tabelsLst:
            df = spark.sql(f'SELECT * FROM {self.base_path}{table}')
            dfs[table] = df           
        return dfs
    
    
    
    def createDimensionCutomer(self):
        """
        Creates the Customer Dimension with surrogate key and validation flags.
        """
        tables = ['source_crm_cust_info','source_erp_cust_az12','source_erp_loc_a101']
        for table in tables:
            df = self.dfs[table]   
            df.createOrReplaceTempView(f'{table}')
            
        

        sql = """
            SELECT 
                ROW_NUMBER() OVER(ORDER BY ci.cst_id) customer_key,
                ci.cst_id AS customer_id,
                ci.cst_key AS customer_number,
                ci.cst_firstname AS first_name,
                ci.cst_lastname AS last_name,
                la.CNTRY AS country,
                ci.cst_marital_status AS marital_status,
                CASE 
                    WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
                    ELSE COALESCE(ca.GEN, 'Unknown')
                END AS gender,
                ca.BDATE AS birth_date,
                ci.cst_create_date AS create_date,
                ca.is_valid_BDATE AS dwh_valid_birth_date
                
            FROM source_crm_cust_info ci 
            LEFT JOIN source_erp_cust_az12 ca
                ON ci.cst_key = ca.CID
            LEFT JOIN source_erp_loc_a101 la
                ON ci.cst_key = la.CID
        """
        result_df = spark.sql(sql)
        return result_df 

    def createDimensionProduct(self):
        """
        Creates the Product Dimension with surrogate key and validation flags.
        """
        tables = ['source_crm_prd_info','source_erp_px_cat_g1v2']
        for table in tables:
            df = self.dfs[table]   
            df.createOrReplaceTempView(table)  

        sql = """
        SELECT 
            ROW_NUMBER() OVER(ORDER BY prd.prd_start_dt, prd.prd_key ) AS product_key,
            prd.prd_id        AS product_id,
            prd.prd_key       AS product_number,
            prd.prd_nm        AS product_name,
            prd.cat_id        AS category_id,
            cat.CAT           AS category_name,
            cat.SUBCAT        AS subcategory_name,
            cat.MAINTENANCE   AS requires_maintenance,
            prd.prd_cost      AS unit_cost,
            prd.prd_line      AS product_line,
            prd.prd_start_dt  AS start_date,
            prd.is_valid_dates AS dwh_valid_dates
        FROM source_crm_prd_info prd
        LEFT JOIN source_erp_px_cat_g1v2 cat 
            ON prd.cat_id = cat.ID
        WHERE prd.prd_end_dt IS NULL;
        """
        result_df = spark.sql(sql)
        return result_df

    def createFactSales(self):
        """
        Creates the Sales Fact table with surrogate keys and validation flags.
        """
        sales_df = self.dfs['source_crm_sales_details']
        
        sales_df.createOrReplaceTempView("sls")
        self.DimensionCutomer.createOrReplaceTempView('cust')
        self.DimensionProduct.createOrReplaceTempView('prod')
        
        sql = """
        SELECT 
            sls.sls_ord_num AS order_number,
            prod.product_key AS product_key,
            cust.customer_key AS customer_key,
            sls.sls_order_dt AS order_date,
            sls.sls_ship_dt AS ship_date,
            sls.sls_due_dt AS due_date,
            sls.sls_sales AS sales_amount,
            sls.sls_quantity AS quantity,
            sls.sls_price AS unit_price,
            sls.is_valid_date_sequence AS dwh_valid_date_sequence
        FROM sls 
        LEFT JOIN prod 
            ON sls.sls_prd_key = prod.product_number
        LEFT JOIN cust
            ON sls.sls_cust_id = cust.customer_id
        """
        result_df = spark.sql(sql)
        return result_df
        
        
    def saveInGold(self):
        """
        Saves Dimension and Fact tables into the Gold Layer as Delta tables.
        """
        base_path = 'workspace.gold'
        dect_final_data_Frames = {
        "dim_cutomer" : self.DimensionCutomer,
        "dim_product" : self.DimensionProduct,
        "fact_sales" : self.FactSales}
        
        for name , df in dect_final_data_Frames.items():
            try:
                df.write.format('delta').mode('overwrite').saveAsTable(f"{base_path}.{name}")
            except Exception as e:
                print(f"Error saving {name}: {e}")
     
    def startGoldlayer(self):
        """Triggers the saving process of the Gold Layer."""
        self.saveInGold()
        
