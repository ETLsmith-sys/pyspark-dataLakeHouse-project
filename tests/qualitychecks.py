from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import * 

spark = (
    SparkSession.builder
    .appName("QualityChecks")
    .enableHiveSupport()
    .getOrCreate()
)

class QualityChecks:
    def __init__(self):
        self.base_path = "workspace.gold"
        self.tablelst = ['dim_cutomer','dim_product','fact_sales']
        self.dataframes = self.readFromGoldLayer()
        
    
    def readFromGoldLayer(self):
        dfs = {}
        for table in self.tablelst:
            dfs[table] = spark.sql(f"SELECT * FROM {self.base_path}.{table}")
        
        return dfs
    
    def duplicatesOrNot(self, df, col):
        """
        Check for duplicate values in a given column.
        
        Args:
            df (DataFrame): The DataFrame to check.
            col (str): Column name to check for duplicates.
        
        Returns:
            DataFrame: Values with duplicates and their counts.
        """
        result_df = df.groupBy(col).count().filter("count > 1")

        return result_df

    def totalNullValues(self,df):
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

    def describeData(self,df, column=None):
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

    def qualityChecks(self):
        dictChecks = {
            "dim_cutomer":'customer_key',
            "dim_product":'product_key',
            "fact_sales": 'order_number'}
        
        for df,pk_col in dictChecks.items():
            self.describeData(self.dataframes[df],pk_col)
            


