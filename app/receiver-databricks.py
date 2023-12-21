# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, expr
from pyspark.sql.functions import col, unbase64


connection_string = f"Endpoint=sb://{namespace}.servicebus.windows.net/;SharedAccessKeyName={accessKeyName};SharedAccessKey={accessKey};EntityPath={eventHubName}"
ehConf={}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)

spark = SparkSession.builder.appName("EventHubsExample").getOrCreate()

# Read from Event Hubs
raw_df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

# Convert the value column from binary to string
raw_df = raw_df.withColumn("value", raw_df["body"].cast("string"))

# Split the data using the "|" delimiter
split_columns = split(raw_df["value"], "\\|")
split_df = raw_df.withColumn("split_data", split_columns)

# Select the split columns and apply the defined schema
parsed_df = split_df.selectExpr("split_data[0] as timestamp", "split_data[1] as log_level", "split_data[2] as request_id", "split_data[3] as session_id", "split_data[4] as user_id", "split_data[5] as action", "split_data[6] as http_method", "split_data[7] as url", "split_data[8] as referrer_url", "split_data[9] as ip_address", "split_data[10] as user_agent", "split_data[11] as response_time", "split_data[12] as product_id", "split_data[13] as cart_size", "split_data[14] as checkout_status", "split_data[15] as token", "split_data[16] as auth_method", "split_data[17] as auth_level", "split_data[18] as correlation_id", "split_data[19] as server_ip", "split_data[20] as port", "split_data[21] as protocol", "split_data[22] as status_and_detail")

# Write the parsed DataFrame to the console sink
parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

display(parsed_df)
#query.awaitTermination()
