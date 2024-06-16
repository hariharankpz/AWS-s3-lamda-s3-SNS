import json
import pandas as pd
import boto3
import io
from datetime import date
import os
from dotenv import load_dotenv

#Loding envs
load_dotenv()

def lambda_handler(event, context):
    # Code added from CI CD
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_key = event['Records'][0]['s3']['object']['key']
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=input_bucket, Key=input_key)
    body = obj['Body'].read()
    json_data = json.loads(body.decode('utf-8'))  # Decode and load JSON data directly
    
    df_list = []
    for py_dict in json_data:
        if py_dict['status'] == 'delivered':
            df_list.append(py_dict)
    
    if df_list:
        df = pd.DataFrame(df_list)
        df.to_csv('/tmp/test.csv', sep=',', index=False)
        print('test.csv file created')

        try:
            date_var = str(date.today())
            file_name =  f'processed_data/{date_var}_processed_data.csv'
            # additional code for further processing (e.g., uploading the file to S3) can be added here
        except Exception as e:
            file_name = 'processed_data/processed_data.csv'
        try:
            lambda_path = '/tmp/test.csv'
            bucket_name = os.getenv('output_bucket')
            s3 = boto3.resource('s3')
            output_bucket_object = s3.Bucket(bucket_name)
            
            output_bucket_object.upload_file(lambda_path,file_name)

            #sns to deliver file processed request 
            sns_client = boto3.client('sns')
            message = "Input S3 File {} has been processed succesfuly !!".format("s3://"+bucket_name+"/"+file_name)
            response = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=os.getenv('sns_arn'), Message=message, MessageStructure='text')

        except Exception as e:
            print("Exception while uploading  the file: ",str(e))
            message = "Input S3 File {} processing is Failed !!".format("s3://"+bucket_name+"/"+file_name)
            respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=os.getenv('sns_arn'), Message=message, MessageStructure='text')
