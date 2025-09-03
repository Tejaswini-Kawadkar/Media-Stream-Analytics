# Media-Stream-Analytics

Here’s a professional and structured **README** file for your Project 2: Media Stream Analytics. You can save this as `README.md` and include it in your GitHub repository.

---

# Media Stream Analytics

## Project Overview

**Media Stream Analytics** is a real-time data pipeline designed to process viewership logs and related metadata for a media streaming platform. This project leverages AWS services including Kinesis, EMR, Glue, S3, Athena, Airflow (MWAA), Snowflake, and QuickSight to ingest, transform, and visualize streaming and batch data.

---

## Problem Statement / Objectives

The project addresses the following tasks:

1. Generate real-time viewership logs and send them to an **AWS Kinesis** stream.
2. Process streaming data using **EMR** and write outputs to **Snowflake** and **S3** in parallel.
3. Load ad revenue, channel metadata, and demographic data into S3 and transform it using **AWS Glue**.
4. Implement **Iceberg tables** to allow incremental updates to the S3 data whenever source files are updated.
5. Use **Lambda + Airflow** (MWAA) to automatically trigger Glue ETL jobs for new or updated files.
6. Query data using **Athena** for analytics purposes.
7. Create interactive dashboards in **QuickSight**, sourcing data from Snowflake.

---

## Architecture Diagram

<img width="768" height="67" alt="Project2_Media_Stream" src="https://github.com/user-attachments/assets/1110725a-df84-489b-a092-4e031e740f01" />

```
[Viewership Logs CSV] --> Kinesis Stream --> EMR Cluster --> Snowflake & S3
                                                 |
                                                 --> Glue transforms metadata/ad revenue/demographics
                                                 |
                                                 --> Iceberg Tables in S3
Lambda --> MWAA (Airflow DAG) --> Glue Job --> Athena Queries --> QuickSight Dashboard
```

---

## Steps & Implementation

### Step 1: Create and Transform Iceberg Tables (Glue / PySpark)

* Initialize Spark & Glue contexts.
* Read metadata files (`demographics.csv`, `ad_revenue.csv`, `channel_metadata.csv`) from S3.
* Define schemas and cast column types.
* Write datasets to Iceberg tables in S3 using Glue Catalog.

```python
# Example: Write demographics to Iceberg table
demographics_df.writeTo("glue_catalog.mydb_tk.demographics").using("iceberg").createOrReplace()
```

---

### Step 2: Lambda Function to Trigger Airflow DAG

* Lambda listens to S3 events for new/updated files.
* Sends HTTP request to MWAA environment to trigger `media_stream_etl` DAG.
* DAG triggers a Glue job to merge or insert updated records into Iceberg tables.

```python
# lambda_function.py
def lambda_handler(event, context):
    # Extract S3 info
    # Authenticate with MWAA
    # Trigger DAG using CLI token
```

---

### Step 3: Airflow & Glue ETL Job

* **Airflow DAG** triggers Glue job to update Iceberg tables.
* Glue ETL job reads CSV updates from S3, merges into Iceberg tables.

```python
# Merge example
MERGE INTO glue_catalog.mydb_tk.ad_revenue target
USING ad_revenue_updates source
ON target.channel_id = source.channel_id AND target.date = source.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

---

### Step 4: Generate Real-Time Data via Kinesis

* EC2 instance runs Python script to read `viewership_logs.csv` and push JSON records to Kinesis stream.
* Partitioned by `region`.

```python
# stream1.py
kinesis.put_record(StreamName=STREAM_NAME, Data=json.dumps(row), PartitionKey=row["region"])
```

---

### Step 5: Process Streaming Data (EMR)

* EMR cluster reads streaming data from Kinesis using PySpark Structured Streaming.
* Writes batch outputs to **Snowflake** and **S3 (Parquet)**.
* Maintains checkpoint for fault tolerance.

```python
query = df_parsed.writeStream.foreachBatch(write_batch).outputMode("append").start()
query.awaitTermination()
```

---

### Step 6: Query Data (Athena)

Sample queries:

1. **Total viewership duration per channel**

```sql
SELECT channel_name, SUM(duration_minutes) AS total_viewership_duration
FROM viewership_logs
GROUP BY channel_name
ORDER BY total_viewership_duration DESC;
```

2. **Gender-wise average completion percentage**

```sql
SELECT d.gender, AVG(v.completion_percentage) AS avg_completion_percentage
FROM viewership_logs v
JOIN demographics d ON v.user_id = d.user_id
GROUP BY d.gender;
```

3. **Peak viewership hours by region**

```sql
SELECT region, hour(cast(timestamp AS timestamp)) AS view_hour, COUNT(*) AS view_count
FROM viewership_logs
GROUP BY region, hour(cast(timestamp AS timestamp))
ORDER BY region ASC, view_count DESC;
```

*Full query list is in the project scripts.*

---

### Step 7: Visualize in QuickSight

* Connect QuickSight to Snowflake dataset.
* Create dashboards to visualize:

  * Total viewership by channel
  * Engagement by device
  * Daily ad revenue
  * Popular genres and shows
  * Age group and subscription insights

---

## Tools & Technologies

* **AWS Services**: Kinesis, EMR, S3, Glue, Athena, MWAA (Airflow), Lambda, QuickSight
* **Data Warehouse**: Snowflake
* **Data Formats**: CSV, Parquet, Iceberg tables
* **Languages & Frameworks**: Python, PySpark, SQL

---

## Key Features

* Real-time streaming analytics with Kinesis and EMR
* Incremental data updates with Iceberg tables
* Automated ETL with Lambda + Airflow + Glue
* Interactive dashboards with QuickSight
* Seamless integration of Snowflake and S3 for batch and streaming data

---

## Project Structure

```
project2/
├── lambda_function.py          # Lambda to trigger Airflow DAG
├── media_stream_etl.py         # Airflow DAG to start Glue jobs
├── media_iceberg_job.py        # Glue ETL job for Iceberg tables
├── stream1.py                  # EC2 script to push data to Kinesis
├── viewership_logs.csv         # Sample CSV data
├── demographics.csv            # Demographics data
├── ad_revenue.csv              # Ad revenue data
├── channel_metadata.csv        # Channel metadata
└── README.md                   # Project documentation
```


