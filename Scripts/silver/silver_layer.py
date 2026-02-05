from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import * 

spark = (
    SparkSession.builder
    .appName("SilverLayer")
    .enableHiveSupport()
    .getOrCreate()
)
class SilverLayer:
    """
    SilverLyer Class
    
    Represents the Silver Layer in the Data Lakehouse.
    - Reads data from the Bronze Layer.
    - Cleans and transforms data using specialized functions for each table.
    - Saves the cleaned results into the Silver Layer as Delta Tables.
    """

    def __init__(self):
        """
        Initialize the SilverLyer class.
        
        - Defines the Bronze Layer path.
        - Loads all required tables from Bronze into memory.
        - Assigns each DataFrame to a class attribute for easy access.
        """
        self.base_path = 'workspace.bronze'
        self.df_lst = ['source_crm_cust_info',
                       'source_crm_prd_info',
                       'source_crm_sales_details',
                       'source_erp_cust_az12',
                       'source_erp_loc_a101',
                       'source_erp_px_cat_g1v2']
        
        self.dfs = self.readFromBronzeLayer()

        self.df_cust_info = self.dfs['source_crm_cust_info']
        self.df_prd_info = self.dfs['source_crm_prd_info']
        self.df_sales_details = self.dfs['source_crm_sales_details']
        self.df_cust_az12 = self.dfs['source_erp_cust_az12']
        self.df_loc_a101 = self.dfs['source_erp_loc_a101']
        self.df_px_cat_g1v2 = self.dfs['source_erp_px_cat_g1v2']
        
    def readFromBronzeLayer(self):
        """
        Read tables from the Bronze Layer.
        
        Returns:
            dict: Dictionary mapping table names to DataFrames.
        """
        dfs = {}
        for table in self.df_lst:
            dfs[table] = spark.sql(f"SELECT * FROM {self.base_path}.{table}")
        return dfs
    
    def duplicatesOrNot(self, df, col, view_name="temp_table"):
        """
        Check for duplicate values in a given column.
        
        Args:
            df (DataFrame): The DataFrame to check.
            col (str): Column name to check for duplicates.
            view_name (str): Temporary view name.
        
        Returns:
            DataFrame: Values with duplicates and their counts.
        """
        result_df = df.groupBy(col).count().filter("count > 1")

        return result_df

    def totalNullValues(self, df):
        """
        Count null values in each column.
        
        Args:
            df (DataFrame): The DataFrame to check.
        
        Returns:
            DataFrame: Number of null values per column.
        """
        nulls = df.select(
            [F.sum(F.when(F.col(c).isNull(),1).otherwise(0)).alias(c) for c in df.columns])
        return nulls

    def describeData(self, df, column=None):
        """
        Provide a descriptive summary of the dataset.
        
        Args:
            df (DataFrame): The DataFrame to describe.
            column (str): Column to check for duplicates.
        """
        print("Data Frame schema: ")
        df.printSchema()
        print("Total Null Values per column:")
        self.totalNullValues(df).show()
        if column is not None:
            print("Duplicates:")
            self.duplicatesOrNot(df, column).show()
        print("Statistical summary:")
        df.describe().show()
        print("Distinct sample:")
        df.distinct().show()
        
    def cust_info_cleaning(self, df):
        """
        Clean customer information data.
        
        - Removes records without cst_id.
        - Drops duplicates based on cst_id.
        - Standardizes gender and marital status values.
        - Cleans and formats customer names.
        
        Returns:
            DataFrame: Cleaned customer info data.
        """
        df = df.dropna(subset=['cst_id'])
        df = df.orderBy(F.col("cst_create_date").desc())
        df = df.dropDuplicates(subset=['cst_id'])

        
        df = df.withColumn(
            'cst_gndr',
            F.when(F.col('cst_gndr').isNull() | (F.trim(F.col('cst_gndr')) == ''), 'Unknown')
            .when(F.upper(F.col('cst_gndr')).startswith('M'), 'Male')
            .when(F.upper(F.col('cst_gndr')).startswith('F'), 'Female')
            .otherwise('Unknown')
        )

        df = df.withColumn(
            'cst_marital_status',
            F.when(F.col('cst_marital_status').isNull() | (F.trim(F.col('cst_marital_status')) == ''), 'Unknown')
            .when(F.upper(F.col('cst_marital_status')).startswith('M'), 'Married')
            .when(F.upper(F.col('cst_marital_status')).startswith('S'), 'Single')
            .otherwise('Unknown')
        )

        lstcols = ['cst_firstname', 'cst_lastname']
        for c in lstcols:
            df = df.withColumn(
                c,
                F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ''), 'Unknown')
                .otherwise(F.initcap(F.trim(F.col(c))))
            )
        return df
     
    def prd_info_cleaning(self, df):
        """
        Clean product information data.
        
        - Extracts cat_id from prd_key.
        - Adjusts prd_key format.
        - Replaces null product cost with 0.
        - Standardizes product line values.
        - Converts date columns to DateType.
        - Adds validity and active flag columns.
        
        Returns:
            DataFrame: Cleaned product info data.
        """
        df = df.withColumn(
            "cat_id",
            F.regexp_replace(F.substring(F.col('prd_key'), 1, 5), '-', '_')
        )
        
        df = df.withColumn(
            "prd_key",
            F.substring(F.col('prd_key'), 7, F.length(F.col('prd_key')))
        )
        
        df = df.withColumn(
            'prd_cost',
            F.when(F.col('prd_cost').isNull(), 0).otherwise(F.col('prd_cost'))
        )
        
        df = df.withColumn(
            "prd_line",
            F.when(F.upper(F.trim(F.col('prd_line'))) == "R", "Road")
            .when(F.upper(F.trim(F.col('prd_line'))) == "M", "Mountain")
            .when(F.upper(F.trim(F.col('prd_line'))) == "S", "Other Sales")
            .when(F.upper(F.trim(F.col('prd_line'))) == "T", "Touring")
            .otherwise("Unknown")
        )
        
        df = df.withColumn('prd_start_dt', F.col('prd_start_dt').cast(DateType()))
        df = df.withColumn('prd_end_dt', F.col('prd_end_dt').cast(DateType()))
        
        df = df.withColumn(
            'is_valid_dates',
            F.when(F.col('prd_start_dt') <= F.col('prd_end_dt'), True).otherwise(False)
        )

        df = df.withColumn(
            'active_flag',
            F.when(F.col('prd_end_dt').isNull(), True).otherwise(False)
        )
        return df
    
    def sales_details_cleaning(self, df):
        """
        Clean sales transaction data.
        
        - Validates and recalculates sls_sales if inconsistent.
        - Adjusts sls_price if invalid or null.
        - Converts order, ship, and due dates to DateType.
        - Adds a column to check valid date sequence.
        
        Returns:
            DataFrame: Cleaned sales details data.
        """
        df = df.withColumn(
            'sls_sales',
            F.when(
                (F.col('sls_sales') <= 0) |
                (F.col('sls_sales').isNull()) |
                (F.col('sls_sales') != (F.abs(F.col('sls_price')) * F.col('sls_quantity'))),
                (F.abs(F.col('sls_price')) * F.col('sls_quantity'))
            ).otherwise(F.col('sls_sales'))
        )
      
        df = df.withColumn(
            "sls_price",
            F.when((F.col("sls_price").isNull()) | (F.col("sls_price") <= 0),
            F.when(F.col("sls_quantity") > 0,
            F.abs(F.col("sls_sales")) / F.col("sls_quantity")))
            .otherwise(F.col("sls_price"))
        )
                
      
        
        lstColumns = ['sls_order_dt','sls_ship_dt','sls_due_dt']
        for c in lstColumns:
            df = df.withColumn(c, F.try_to_date(F.col(c), 'yyyyMMdd'))

            

        df = df.withColumn(
            'is_valid_date_sequence',
            F.when(
                (F.col('sls_order_dt') <= F.col('sls_ship_dt')) &
                (F.col('sls_ship_dt') <= F.col('sls_due_dt')),
                True
            ).otherwise(False)
        )
        return df
    
    def cust_az12_cleaning(self, df):
        """
        Clean customer data from cust_az12 table.
        
        - Adjusts CID by removing NAS prefix if present.
        - Standardizes GEN values (Male, Female, Unknown).
        - Calculates age from BDATE and validates it (between 18 and 100).
        - Adds is_valid_BDATE flag.
        - Sets BDATE to null if greater than today.
        
        Returns:
            DataFrame: Cleaned cust_az12 data.
        """
        today = F.current_date()
        age = F.when(F.col("BDATE").isNotNull(),
                    F.floor(F.months_between(F.current_date(), F.col("BDATE")) / 12))
        
        df = df.withColumn(
            'CID',
            F.when(F.col('CID').like('NAS%'),
                   F.substring(F.col('CID'), 4, F.length(F.col('CID'))))
            .otherwise(F.col('CID'))
        )
        
        df = df.withColumn(
            'GEN',
            F.when(F.upper(F.trim(F.col('GEN'))).isin(['M','MALE']), "Male")
            .when(F.upper(F.trim(F.col('GEN'))).isin(['F','FEMALE']), "Female")
            .otherwise("Unknown")
        )
        
        df = df.withColumn(
            "is_valid_BDATE",
            F.when((F.col('BDATE') <= today) & age.between(18, 100), True)
            .otherwise(False)
        )
         
        df = df.withColumn(
            'BDATE',
            F.when(F.col('BDATE') >= today, F.lit(None))
            .otherwise(F.col('BDATE'))
        )
        return df
    
    def loc_a101_cleaning(self, df):
        """
        Clean location data from loc_a101 table.
        
        - Removes dashes from CID.
        - Standardizes CNTRY values:
            * "US" or "UNITED STATES" → "USA"
            * "DE" → "Germany"
            * Null or empty → "Unknown"
            * Otherwise trims whitespace.
        
        Returns:
            DataFrame: Cleaned loc_a101 data.
        """
        df = df.withColumn("CID",
            F.regexp_replace(F.col('CID'), '-', '') 
        )
        
        df = df.withColumn(
            'CNTRY',
            F.when((F.col('CNTRY').isNull()) | (F.trim(F.col('CNTRY')) == ""), "Unknown")  
            .when(F.upper(F.trim(F.col('CNTRY'))).isin(["US", "UNITED STATES"]), "USA") 
            .when(F.upper(F.trim(F.col('CNTRY'))) == "DE", "Germany")
            .otherwise(F.trim(F.col('CNTRY')))
        )
        return df

    def saveInSilver(self):
        """
        Save cleaned DataFrames into the Silver Layer.
        
        - Creates a dictionary mapping cleaned DataFrames to target Silver table names.
        - Iterates through the dictionary and writes each DataFrame as a Delta Table.
        - Uses overwrite mode to replace existing tables.
        - Includes error handling to print messages if saving fails.
        """
        base_path = "workspace.silver"  
        dataCleaningDict = {
           'source_crm_cust_info': self.cust_info_cleaning(self.df_cust_info),
           'source_crm_prd_info': self.prd_info_cleaning(self.df_prd_info),
           'source_crm_sales_details': self.sales_details_cleaning(self.df_sales_details),
           'source_erp_cust_az12_clean': self.cust_az12_cleaning(self.df_cust_az12),
           'source_erp_loc_a101': self.loc_a101_cleaning(self.df_loc_a101),
           'source_erp_px_cat_g1v2': self.df_px_cat_g1v2
        }
        for name, dfClaning in dataCleaningDict.items():
            try:
                dfClaning.write.format('delta').mode('overwrite').saveAsTable(f"{base_path}.{name}")
            except Exception as e:
                print(f"Error saving {name}: {e}")

    def startSilverlayer(self):
        """
        Entry point to trigger the Silver Layer pipeline.
        
        - Calls saveInSilver() to clean and save all datasets into the Silver Layer.
        """
        self.saveInSilver()
