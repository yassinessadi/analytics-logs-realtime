# Databricks notebook source
# MAGIC %pip install azure-eventhub

# COMMAND ----------

# MAGIC %md
# MAGIC <code> 
# MAGIC restart the kernel:
# MAGIC </code>

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC <code>
# MAGIC import libs:
# MAGIC </code>

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from azure.eventhub import EventHubConsumerClient

# COMMAND ----------



# COMMAND ----------

namespace = "regulargazelleahee"
accessKeyName = "RootManageSharedAccessKey"
accessKey = "WAaU90f+XdiB/SrFEO4+0p8VfNQv8PP40+AEhFAwLTc="
eventHubName = "logs-handler"
connection_string = f"Endpoint=sb://{namespace}.servicebus.windows.net/;SharedAccessKeyName={accessKeyName};SharedAccessKey={accessKey};EntityPath={eventHubName}"
starting_position = "-1"  # Start reading from the latest available offset

# COMMAND ----------

# MAGIC %md
# MAGIC <code>
# MAGIC Init spark session:
# MAGIC </code>

# COMMAND ----------

# Create a SparkSession
spark = SparkSession.builder \
    .appName("EventHubsStreaming") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC <code>
# MAGIC StructType:
# MAGIC </code>

# COMMAND ----------

log_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("log_level", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("http_method", StringType(), True),
    StructField("url", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("response_time", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("cart_size", IntegerType(), True),
    StructField("checkout_status", StringType(), True),
    StructField("token", StringType(), True),
    StructField("auth_method", StringType(), True),
    StructField("auth_level", StringType(), True),
    StructField("correlation_id", StringType(), True),
    StructField("server_ip", StringType(), True),
    StructField("port", IntegerType(), True),
    StructField("protocol", StringType(), True),
    StructField("status_and_detail", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC <code> 
# MAGIC read stream with spark streaming:
# MAGIC </code>

# COMMAND ----------

def readStream(read_options):
    df = spark \
    .readStream \
    .format("eventhubs") \
    .options(**read_options) \
    .load()
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC <code>
# MAGIC Define the streaming read options:
# MAGIC </code>

# COMMAND ----------

read_options = {
    "eventhubs.connectionString": connection_string,
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": starting_position
}

# COMMAND ----------

# MAGIC %md
# MAGIC <code>
# MAGIC  Convert the value column from binary to string
# MAGIC </code>

# COMMAND ----------

df = readStream(read_options)
df = df.withColumn("value", df["body"].cast("string"))

# COMMAND ----------

# MAGIC %md
# MAGIC <code>
# MAGIC create schema to struct the data with structtype:
# MAGIC </code>

# COMMAND ----------

df = df.withColumn("data", from_json(df["value"], log_schema))

# COMMAND ----------

# MAGIC %md
# MAGIC <code>Extract the fields from the JSON data</code>

# COMMAND ----------

df = df.select("data.*")

# COMMAND ----------

# MAGIC %md
# MAGIC <code>Wait for the streaming query to finish:</code>

# COMMAND ----------

df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
spark.streams.awaitAnyTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC <code>
# MAGIC Perform further data processing or analysis on the streaming DataFrame:
# MAGIC </code>

# COMMAND ----------

#def on_event(partition_context, event):
    # Process the received event
#    print("Received event:", event.body_as_str())

#consumer_client = EventHubConsumerClient.from_connection_string(connection_string, consumer_group="$Default")

#with consumer_client:
#    consumer_client.receive(on_event=on_event, eventhub_name=eventHubName)
