import json
import pandas as pd
import boto3
import io
from datetime import date
import os
from dotenv import load_dotenv

#Loding envs
load_dotenv()

s3 = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):


    # Specify your source and destination bucket names
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    # destination_bucket = os.getenv('output_bucket')
    destination_bucket = " hh-doordash-target-zn-gds-assign-3"
    sns_arn = "arn:aws:sns:us-east-1:381491939671:s3-lambda-file-execution"
    file_key = event['Records'][0]['s3']['object']['key']  # JSON file key in the source bucket


    print("destination_bucket :::::",destination_bucket)
    # Read JSON file from S3 into Pandas DataFrame
    response = s3.get_object(Bucket=source_bucket, Key=file_key)
    json_data = response['Body'].read().decode('utf-8')
    data = json.loads(json_data)
    print("data ::::::::",data)
    df = pd.DataFrame(data)

    # Convert DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    print("-----------PRINTING DATAFRAME--------",df)


    try:
        date_var = str(date.today())
        output_file_key =  f'processed_data/{date_var}_processed_data.csv'
    # additional code for further processing (e.g., uploading the file to S3) can be added here
    except Exception as e:
        print("FILE NAME NOT GENERATED BASE ON DATE TIME")
        output_file_key = 'processed_data/processed_data.csv'
       
    try: 
        print("TARGET ARN: ",os.getenv('sns_arn'))
        # Upload CSV file to S3
        s3.put_object(Bucket=destination_bucket, Key=output_file_key, Body=csv_buffer.getvalue())
        
        # output_bucket_object.upload_file(lambda_path,file_name)
        print("FILE UPLOADED SUCCESFULLY")

        #sns to deliver file processed request 
        
        message = f"Input S3 File s3://{destination_bucket}/{output_file_key} has been processed successfully!!"
        response = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=os.getenv('sns_arn'), Message=message, MessageStructure='text')

    except Exception as e:
        print("Exception while uploading  the file: ",str(e))
        error_message = f"Input S3 File {destination_bucket}/{output_file_key} processing failed !! Error: {e}"
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=os.getenv('sns_arn'), Message=error_message, MessageStructure='text')

    try:
        # Save DataFrame to CSV file
        csv_file = '/tmp/data.csv'  # Temporary local path
        df.to_csv(csv_file, index=False)
        # Upload CSV to S3 bucket
        with open(csv_file, 'rb') as f:
            s3.put_object(Bucket=destination_bucket, Key=output_file_key, Body=f)
        print("FILE UPLOADED SUCCESFULLY AT SECOND FUNCTION")
    except Exception as e:
        print("Exception occurs :",str(e))