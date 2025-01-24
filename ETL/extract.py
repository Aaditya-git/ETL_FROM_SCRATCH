import boto3
# from S3_connection.cloud_connector import S3_connector, input_bucket_name, fetching_bucket

from S3_connection.cloud_connector import S3_connector



class Extract:

    def copy_from_one_bucket_to_another(self,source_bucket,output_bucket,file_name):
            print("Inside copy function")
            s3 = boto3.resource('s3')
            copy_source = {
                'Bucket': source_bucket,
                'Key': file_name
            }
            s3.meta.client.copy(copy_source, output_bucket, f'bronze/{file_name}')


if __name__ =="__main__":

    #write a YAML for variables differently
    # variable 1
    file_name_csv='winemag.csv'
    extract = Extract()
    extract.copy_from_one_bucket_to_another('input-source-bucket-for-etl','',file_name_csv) 
    # Need to write my logic here