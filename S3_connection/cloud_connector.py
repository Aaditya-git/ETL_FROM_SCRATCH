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

         #check if file exists or not 
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_name} does not exist in {hard_path}")

        #Generating file path
        folder_to_be_uploaded = folder_name
        key = file_name
        final_name_path = f"{folder_to_be_uploaded}/{key}"
        print(final_name_path)

        #In which bucket you want to upload, need to add if bucket exists or not exception 
        try:
            bucket_name = buk_name
            res = self.s3_client.head_bucket(Bucket=buk_name)
            print(f"Bucket {buk_name} exists")
            #Need to add exceptions
            self.s3_client.upload_file(file_path,bucket_name,final_name_path)
        except Exception as e:
            print(e)
    
    def download(self):
        pass




