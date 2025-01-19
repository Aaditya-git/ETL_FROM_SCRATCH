import boto3
from S3_connection.cloud_connector import S3_connector

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *

class Spark_cluster:
    def __init__(self):
        try:
            self.spark = SparkSession.builder.master("local[5]").appName("testing").getOrCreate()
            
        except Exception as e:
            print(e)

    #rollback is needed.
    #dont want to download the file again and again.
    def read_and_write_dataframe(self):
        try:
            S3_connector.download(self, 'winemag.csv')
            # Correct the file path for Windows
            file_path = r'C:\Clutchgod\S3_connector\downloaded_files\raw_file.csv'
            # Read the CSV file into a Spark DataFrame
            df = self.spark.read.format('csv') \
                        .option('inferSchema', True) \
                        .option('header', True) \
                        .load(file_path)
        except Exception as e:
            print(f"Error in read_dataframe: {e}")

    def silver_layer_transformation(self):
        #after transformation internal call to save file or upload file to silver folder
        pass
    
if __name__ =="__main__":
    clust = Spark_cluster()
    clust.read_dataframe()
