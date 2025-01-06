from S3_connection.cloud_connector import S3_connector, input_bucket_name,fetching_bucket

obj=S3_connector()
obj.create_buck_if_not_exists(fetching_bucket)
obj.upload('datta.csv','test_kay_boltodadas','bronze')