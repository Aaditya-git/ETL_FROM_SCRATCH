import boto3
class S3_connector:

    s3_client = boto3.client('s3')
    def create_buck_if_not_exists(self,name):
        try:
            print(f"Received bucket name: {name}")
            res=self.s3_client.head_bucket(Bucket=name)
            print(res)
            print(f"Bucket '{name}' already exists.")

        except Exception as e:
            print("inside exception")
        
            print('bucket does not exists... creating ....')
            response = self.s3_client.create_bucket(
                ACL='private',
                Bucket=name
            )

    def upload(self):
        self.s3_client.upload_file('C:\Clutchgod\S3_connector\BigMart Sales.csv','test-s3-bucket-1953','raw_Data/Bigmart_sales.csv')


bucket = S3_connector()
buck_name='test-s3-bucket-1953'
bucket.create_buck_if_not_exists(buck_name)
bucket.upload()
