# Visual ETL Job

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import to_date, col
from pyspark.sql import SparkSession

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Iceberg catalog and warehouse config
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://tejaswini-b2-sydney/iceberg_tb/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Read CSV from S3 with header and inferSchema false (we'll cast manually)
df = spark.read.option("header", True).csv("s3://tejaswini-b2-sydney/project2/ad_revenue.csv")

# Cast columns with correct types
df_casted = df.withColumn("date", to_date(col("date"), "dd-MM-yyyy")) \
            .withColumn("ad_revenue", col("ad_revenue").cast("double"))

# Register as temp view for SQL merge
df_casted.createOrReplaceTempView("ad_revenue_updates")

# Merge into Iceberg table
spark.sql("""
MERGE INTO glue_catalog.mydb_tk.ad_revenue target
USING ad_revenue_updates source
ON target.channel_id = source.channel_id AND target.date = source.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
