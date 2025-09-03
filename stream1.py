import boto3
import csv
import json
import time

# AWS Kinesis Configuration
STREAM_NAME = "my_data_stream"
REGION = "ap-southeast-2"

# Initialize boto3 client
kinesis = boto3.client("kinesis", region_name=REGION)

# Open the CSV file
with open("viewership_logs.csv", mode="r") as file:
reader = csv.DictReader(file)

for row in reader:
  # Optional: convert data types (e.g., float, bool, int)
  row["duration_minutes"] = int(row["duration_minutes"])
  row["ads_watched"] = int(row["ads_watched"])
  row["ad_revenue"] = float(row["ad_revenue"])
  row["engagement_score"] = float(row["engagement_score"])
  row["buffer_count"] = int(row["buffer_count"])
  row["completion_percentage"] = float(row["completion_percentage"])
  row["is_live"] = True if row["is_live"].lower() == "true" else False
  
  # Send to Kinesis as JSON
  kinesis.put_record(
    StreamName=STREAM_NAME,
    Data=json.dumps(row),
    PartitionKey=row["region"] # Use region as partition key
  )
  
print(f"Sent to Kinesis: {row['session_id']}")
time.sleep(1)
