import json
import boto3
import urllib3
from datetime import datetime

# Your MWAA and DAG config
MWAA_ENV_NAME = "MyAirflowEnvironment_Name"
DAG_NAME = "media_stream_etl"

def lambda_handler(event, context):
    print(" Received S3 Event:", json.dumps(event))
    # Extract S3 bucket and key from the event
  record = event['Records'][0]
  bucket = record['s3']['bucket']['name']  
  key = record['s3']['object']['key']

  # Only keep the folder name, assuming new files always land in the same path
  folder_prefix = "project2/" # hardcoded as per your request
  # Authenticate with MWAA
  mwaa = boto3.client('mwaa', region_name='ap-southeast-2')
  resp = mwaa.create_cli_token(Name=MWAA_ENV_NAME)
  web_token = resp['CliToken']
  web_server_hostname = resp['WebServerHostname']
  print(" Token and hostname retrieved.")
  # Prepare the CLI command
  execution_date = datetime.utcnow().isoformat()
  conf_payload = {
    "bucket": bucket,
    "folder_prefix": folder_prefix
  }
  # --conf must be a stringified JSON
  conf_str = json.dumps(conf_payload)
  cli_command = f"dags trigger -e {execution_date} {DAG_NAME} --conf '{conf_str}'"
  trigger_url = f"https://{web_server_hostname}/aws_mwaa/cli"
  # Make the HTTP request
  http = urllib3.PoolManager()
  headers = {
    "Authorization": f"Bearer {web_token}",
    "Content-Type": "text/plain"
  }
  response = http.request(
    "POST",
    trigger_url,
    headers=headers,
    body=cli_command.encode("utf-8")
  )
  print(" HTTP Status:", response.status)
  print(" Response body:", response.data.decode("utf-8"))
  return {
    'statusCode': response.status,
    'body': f"DAG trigger attempted with status {response.status}"
  }
