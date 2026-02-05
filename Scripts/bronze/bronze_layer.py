from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import * 

spark = (
    SparkSession.builder
    .appName("BronzeLayer")
    .enableHiveSupport()
    .getOrCreate()
)


class BronzeLayer:
    """
    BronzeLyer Class
    ----------------
    Responsible for ingesting raw CSV files from source systems (CRM, ERP)
    and saving them into the Bronze Layer as Delta Tables.

    Attributes:
        base_path (str): Base directory path for source datasets.
        source_dict (dict): Dictionary mapping source systems to their file names.
    """
    def __init__(self):
        self.base_path = "/Volumes/workspace/source_system/dataset/"
        self.source_dict = {'source_crm' : 
            ['cust_info',
             'prd_info',
             'sales_details'],
            
            'source_erp' : 
            ['CUST_AZ12',
             'LOC_A101',
             'PX_CAT_G1V2']}
    

    def readsource_system_AndSaveInBronze(self):
        """ 
        Reads CSV files from source_dict and 
        saves them as Delta Tables. 
        Process: - Iterates over each source system and its files. 
        - Reads CSV files with header and inferred schema. 
        - Saves each DataFrame as a Delta Table in the metastore.
        - Handles exceptions by printing error messages. 
        Output: Delta Tables named as <source>_<file>, e.g.'source_crm_cust_info'. 
            
        """   
        for folder,files in self.source_dict.items():
            for file in files :
                try:
                    df = spark.read.format('csv')\
                        .option('header', 'True')\
                        .option('inferSchema','True')\
                        .load(f'{self.base_path}{folder}/{file}.csv')
                
                    df.write.format('delta').mode('overwrite').saveAsTable(f'workspace.bronze.{folder}_{file}')
            
                except Exception as e :
                    print(f"Error loading {file}: {e}")
    
        
    def startbronzelyer(self):
        """
        Entry point for the Bronze Layer ETL pipeline.

        Purpose:
            - Provides a simple interface to trigger ingestion of raw source data.
            - Calls `readsource_system_AndSaveInBronze` to read CSV files from source systems
            and save them as Delta Tables in the Bronze Layer.

        Output:
            Delta Tables created in the metastore for each source file defined in source_dict.
        """
        self.readsource_system_AndSaveInBronze()



