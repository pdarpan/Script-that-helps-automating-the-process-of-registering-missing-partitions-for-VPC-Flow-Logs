# AWS Security Lake Partition Registration Helper

## Overview
This utility script helps automates the process of registering missing partitions for VPC Flow Logs (version 1.0) in AWS Security Lake. It generates Athena ALTER TABLE statements to register partitions for multiple AWS accounts across different regions.

## Problem Statement
When AWS Security Lake delivers VPC Flow logs to S3, sometimes the partitions may not be automatically registered in Athena, making the data non-queryable. Manual partition registration becomes challenging when dealing with:
- Multiple AWS accounts (hundreds of accounts)
- Multiple regions
- Extended time periods
- Multiple S3 buckets

## Solution
This script generates Athena partition registration statements by:
1. Taking input parameters for account IDs, regions, and bucket names
2. Generating partition statements for a specified date range
3. Batching the statements (500 per file) to avoid Athena query limitations
4. Creating separate files for each region and batch

## Usage
1. Update the following variables in the script:
   - `account_ids`: List of AWS account IDs
   - `regions`: List of AWS regions where VPC Flow logs are collected
   - `bucket_names`: Corresponding S3 bucket names for each region
   - `start`: Start date for partition registration
   
2. Run the script to generate SQL files
3. Execute the generated SQL files in Athena

## Output
The script generates multiple text files named `{region}-{batch_number}.txt` containing Athena ALTER TABLE statements.

## Prerequisites
- Python 3.12 and Greater
- Access to AWS Athena
- Proper permissions to alter tables in Athena
