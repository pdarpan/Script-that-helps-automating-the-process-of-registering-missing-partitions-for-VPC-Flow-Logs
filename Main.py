from itertools import batched
import datetime

# List of AWS account IDs that need partition registration
account_ids = [
    "111111111111",
    "222222222222",
    # ... more account IDs
]

# List of AWS regions where VPC Flow logs are collected
regions = ['<Region-ID-1>', '<Region-ID-2>', '<Region-ID-3>', '<Region-ID-4>']

# Corresponding S3 bucket names for each region
bucket_names = ['amzn-s3-demo-bucket1', 'amzn-s3-demo-bucket2', 'amzn-s3-demo-bucket3', 'amzn-s3-demo-bucket4']

# Start date for partition registration
start = datetime.datetime.strptime("MM-DD-YYYY", "%m-%d-%Y")

# Generate a list of dates for the next 31 days
date_generated = [start + datetime.timedelta(days=x) for x in range(0, 31)]
# Convert dates to required format YYYYMMDD
eventdays = [date.strftime("%Y%m%d") for date in date_generated]

# Process each region
for index in range(0, len(regions)):
    region = regions[index]
    # Format region name for table naming (replace hyphens with underscores)
    formatted_region = region.replace("-", "_")
    bucket_name = bucket_names[index]
    # Generate table name as per Security Lake naming convention
    table_name = f"amazon_security_lake_table_{formatted_region}_vpc_flow_1_0"
    partition_queries = []

    # Generate partition statements for each account and date combination
    for account_id in account_ids:
        for eventday in eventdays:
            partition_query = f"PARTITION (region = '{region}', accountid = '{account_id}', eventday= '{eventday}') LOCATION 's3://{bucket_name}/aws/VPC_FLOW/1.0/region={region}/accountId={account_id}/eventDay={eventday}/'\n"
            partition_queries.append(partition_query)
    
    # Batch queries into groups of 500 to avoid Athena limitations
    batched_list = list(batched(partition_queries, 500))
    
    # Create separate files for each batch
    for batch_index in range(0, len(batched_list)):
        batch = batched_list[batch_index]
        final_query_string = f"ALTER TABLE {table_name} ADD IF NOT EXISTS\n"
        for query in batch:
            final_query_string += query
        # Write the batched queries to region and batch specific files
        with open(f"{region}-{batch_index}.txt", "w") as f:
            f.write(final_query_string)
