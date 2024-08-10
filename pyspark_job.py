import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

s3_file_path =  sys.argv[1]

print(f"Processing file: {s3_file_path}")
# Create a Spark session
spark = SparkSession.builder \
    .appName("Transforming Food Delivery Data") \
    .config("spark.jars.packages", "io.github.spark-redshift-community:spark-redshift_2.12:4.0.0,com.amazonaws:aws-java-sdk:1.11.900,org.apache.hadoop:hadoop-aws:3.2.0") \
    .getOrCreate()

# Define schema
schema = StructType([
            StructField('order_id', IntegerType(), True),
            StructField('customer_id', IntegerType(), True),
            StructField('restaurant_id', IntegerType(), True),
            StructField('order_time', TimestampType(), True),
            StructField('customer_location', StringType(), True),
            StructField('restaurant_location', StringType(), True),
            StructField('order_value', DoubleType(), True),
            StructField('rating', DoubleType(), True),
            StructField('delivery_time', TimestampType(), True)
        ])

        # Read the data from S3
df = spark.read.csv(s3_file_path, schema=schema, header=True)
df.printSchema()

        # Validate the data
df_validated = df.filter(
            (df.order_time.isNotNull()) &
            (df.delivery_time.isNotNull()) &
            (df.order_value > 0)
        )

        # Transform the data
df_transformed = df_validated.withColumn('delivery_duration',
                                                 (df_validated.delivery_time - df_validated.order_time).cast('long') / 60)
df_transformed.show(4)

        # Categorize orders based on value
low_threshold = 500
high_threshold = 1200

df_transformed = df_transformed.withColumn('order_category',
                                                   when(col('order_value') < low_threshold, "Low")
                                                   .when((col('order_value') >= low_threshold) & (col('order_value') <= high_threshold), 'Medium')
                                                   .otherwise("High"))

df_transformed.show(4)

        # Writing the transformed data into a staging area in Amazon S3
output_s3_path = 's3://food-delivery-project/output-files/first-file.csv'
df_transformed.write.csv(output_s3_path,mode="overwrite",header=True)

username = 'awsuser'
password ='Welcome_123'
jdbc_url = f"jdbc:redshift://redshift-cluster-projects.ckbo8kfqmsym.us-east-1.redshift.amazonaws.com:5439/dev?user={username}&password={password}"
aws_iam_role = "arn:aws:iam::025066280149:role/Redshift-Access"
temp_dir="s3://food-delivery-project/temp-folder/"
target_table = 'food_delivery'
table_schema = "order_id int, customer_id int, restaurant_id int, order_time timestamp, customer_location string, restaurant_location string, order_value double, rating double, delivery_time timestamp"
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {target_table} ({table_schema})
    USING io.github.spark_redshift_community.spark.redshift
    OPTIONS (
        url '{jdbc_url}',
        tempdir '{temp_dir}',
        dbtable '{target_table}',
        aws_iam_role '{aws_iam_role}'
    )
"""


# Now you can execute the create_table_query using Spark SQL
spark.sql(create_table_query).show()
#writing data to redshift with options
df_transformed.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url",jdbc_url) \
        .option("dbtable",target_table) \
        .option("tempdir",temp_dir) \
        .option("aws_iam_role", aws_iam_role) \
        .mode("overwrite") \
        .save()
print(f"Data written to Redshift table: {target_table}")
spark.stop()