import boto3
import os 


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

    # takes the name of the file to be uploaded, bucket name, and key i.e. folder name as input 
    def upload(self,file_name,buk_name,folder_name):
        #construct file path
        hard_path = r'C:\Clutchgod\S3_connector'
        file_path = os.path.join(hard_path,file_name)
        print(file_path)

        #  check if file exists or not 
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_name} does not exist in {hard_path}")

        #Generating file path
        folder_to_be_uploaded = folder_name
        key = file_name
        final_name_path = f"{folder_to_be_uploaded}/{key}"
        print(final_name_path)

        #Check if the bucket exists or not! 
        try:
            bucket_name = buk_name
            self.s3_client.head_bucket(Bucket=buk_name)
            print(f"Bucket {buk_name} exists")
            self.s3_client.upload_file(file_path,bucket_name,final_name_path)
        except self.s3_client.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"Bucket '{bucket_name}' does not exist.")
            elif error_code == '403':
                print(f"Bucket '{bucket_name}' exists, but you do not have access to it.")
            else:
                print(f"Error accessing bucket '{bucket_name}': {e}")
    
    def download(self):
        pass



input_bucket_name = 'input-source-bucket-for-etl'
fetching_bucket = 'medallion-bucket'




