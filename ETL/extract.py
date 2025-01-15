from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[5]")\
                    .appName("testing").getOrCreate()

print("this is spark",spark)


# from S3_connection.cloud_connector import S3_connector, input_bucket_name, fetching_bucket
from S3_connection.cloud_connector import S3_connector, input_bucket_name, fetching_bucket

print('this is input bucket name',input_bucket_name)
# obj=S3_connector()


if __name__ =="__main__":
    pass 

