import boto3

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *

class Spark_cluster:
    def __init__(self):
        try:
            spark = SparkSession.builder.master("local[5]")\
                                .appName("testing").getOrCreate()
            
        except Exception as e:
            print(e)


    def read_dataframe(self):
        pass

    def silver_layer_transformation(self):
        #after transformation internal call to save file or upload file to silver folder
        pass
    
    #call to load function t
