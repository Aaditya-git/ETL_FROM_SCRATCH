from S3_connection.cloud_connector import S3_connector

obj=S3_connector()
# obj.create_buck_if_not_exists("my-test-bucket-number-3")
obj.upload('datta.csv','test_kay_bolto','bronze')