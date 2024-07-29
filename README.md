# JSON to CSV Lambda Processor

## Overview

This AWS Lambda function processes JSON files uploaded to an S3 bucket, converts them to CSV format, and stores them back in S3. It also sends email notifications upon successful processing or failure.

## How It Works

1. **Trigger:** The Lambda function is triggered by an S3 event whenever a JSON file is uploaded to the specified S3 bucket.
2. **Processing:**
   - Reads the JSON file from S3.
   - Converts the JSON data to CSV format.
   - Stores the resulting CSV file back in S3.
3. **Notification:** Sends an email notification via AWS SNS to inform about the success or failure of the ingestion process.

## Setup Instructions

### Prerequisites

- AWS account
- AWS Lambda permissions
- S3 bucket
- AWS SNS topic for notifications

### Deployment

1. **Create an S3 Bucket:**
   - Create an S3 bucket to store JSON and CSV files.

2. **Create an SNS Topic:**
   - Create an SNS topic for sending email notifications.
   - Subscribe your email address to the SNS topic.

3. **Deploy the Lambda Function:**
   - Deploy the Lambda function using the provided code.
   - Set up the Lambda function to be triggered by S3 events for the specified bucket.
   - Configure environment variables for the SNS topic ARN.

4. **Permissions:**
   - Ensure the Lambda function has permissions to read from and write to the S3 bucket.
   - Grant permissions to publish messages to the SNS topic.

### Environment Variables

- `SNS_TOPIC_ARN`: The ARN of the SNS topic to send notifications.
