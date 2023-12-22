# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, expr, split
from pyspark.sql.functions import col, unbase64

namespace = "regulargazelleseventhub"
accessKeyName = "RootManageSharedAccessKey"
accessKey = "s4ps7/DF/iv+Vy/gn699+Gaya2fzyfmCL+AEhBu86Z0="
eventHubName = "handler-logs"
starting_position = -1

connection_string = f"Endpoint=sb://{namespace}.servicebus.windows.net/;SharedAccessKeyName={accessKeyName};SharedAccessKey={accessKey};EntityPath={eventHubName}"

ehConf = {
    "eventhubs.consumerGroup": "$Default",
    #"eventhubs.startingPosition": starting_position
}

ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)

spark = SparkSession.builder.appName("EventHubslogshandler").getOrCreate()

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
parsed_df = split_df.selectExpr("split_data[0] as timestamp", "split_data[1] as log_level", "split_data[2] as request_id", "split_data[3] as session_id", "split_data[4] as user_id", "split_data[5] as action", "split_data[6] as http_method", "split_data[7] as url", "split(split_data[8],':') as referrer_url", "split_data[9] as ip_address", "split_data[10] as user_agent", "split_data[11] as response_time", "split_data[12] as product_id", "split_data[13] as cart_size", "split_data[14] as checkout_status", "split_data[15] as token", "split_data[16] as auth_method", "split_data[17] as auth_level", "split_data[18] as correlation_id", "split_data[19] as server_ip", "split_data[20] as port", "split_data[21] as protocol", "split_data[22] as status", "split_data[23] as detail")



# Write the parsed DataFrame to the console sink
parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

display(parsed_df)
#query.awaitTermination()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, expr,concat_ws
from pyspark.sql.functions import col, unbase64

namespace = "regulargazelleseventhub"
accessKeyName = "RootManageSharedAccessKey"
accessKey = "s4ps7/DF/iv+Vy/gn699+Gaya2fzyfmCL+AEhBu86Z0="
eventHubName = "handler-logs"
starting_position = -1

connection_string = f"Endpoint=sb://{namespace}.servicebus.windows.net/;SharedAccessKeyName={accessKeyName};SharedAccessKey={accessKey};EntityPath={eventHubName}"

ehConf = {
    "eventhubs.consumerGroup": "$Default",
    #"eventhubs.startingPosition": starting_position
}

ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)

spark = SparkSession.builder.appName("EventHubslogshandler").getOrCreate()

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
parsed_df = split_df.selectExpr("split_data[0] as timestamp", "split_data[1] as log_level", "split_data[2] as request_id", "split_data[3] as session_id", "split_data[4] as user_id", "split_data[5] as action", "split_data[6] as http_method", "split_data[7] as url", "split_data[8] as referrer_url", "split_data[9] as ip_address", "split_data[10] as user_agent", "split_data[11] as response_time", "split_data[12] as product_id", "split_data[13] as cart_size", "split_data[14] as checkout_status", "split_data[15] as token", "split_data[16] as auth_method", "split_data[17] as auth_level", "split_data[18] as correlation_id", "split_data[19] as server_ip", "split_data[20] as port", "split_data[21] as protocol", "split_data[22] as status", "split_data[23] as detail")

# ------------------------
# realtime transformation
#-------------------------

parsed_df = parsed_df.select(
    split(col("timestamp"),"\"").getItem(1).alias("timestamp"),
    split(col("ip_address"), ":").getItem(1).alias("ip_address"),
    split(col("session_id"), ":").getItem(1).alias("session_id"),
    split(col("Referrer_url"), "\\.").getItem(1).alias("Referrer_url"),
    split(col("cart_size"), ":").getItem(1).alias("cart_size"),
    split(col("checkout_status"), ":").getItem(1).alias("checkout_status"),
    split(col("product_id"), ":").getItem(1).alias("product_id"),
    split(col("response_time"), ":\\s").getItem(1).alias("response_time"),
    split(col("correlation_id"), ":").getItem(1).alias("correlation_id"),
    split(col("auth_method"), ":").getItem(1).alias("auth_method"),
    split(col("auth_level"), ":").getItem(1).alias("auth_level"),
    split(col("Token"), ":").getItem(1).alias("Token"),
    split(col("port"), ":").getItem(1).alias("port"),
    split(col("status"), ":").getItem(2).alias("status"),
    split(col("protocol"), ":").getItem(1).alias("protocol"),
    split(col("detail"), ":").getItem(1).alias("detail"),
    split(col("user_agent"), ":").getItem(1).alias("user_agent"),
)
    # concat_ws("",
    #     split(col("Referrer_url"), ":").getItem(1)),
    #     split(col("Referrer_url"), ":").getItem(2),
    # ).alias("Referrer")


# Write the parsed DataFrame to the console sink
parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

display(parsed_df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql import functions as F


# Create a SparkSession
spark = SparkSession.builder \
    .appName("movies-ratings-app") \
    .config("spark.jars.packages", "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22") \
    .getOrCreate()

    # spark version 3.3.2
    # scala version  2.12
    # lib version 2.3.22
    
namespace = "regulargazelleseventhub"
accessKeyName = "RootManageSharedAccessKey"
accessKey = "s4ps7/DF/iv+Vy/gn699+Gaya2fzyfmCL+AEhBu86Z0="
eventHubName = "handler-logs"


connection_string = f"Endpoint=sb://{namespace}.servicebus.windows.net/;SharedAccessKeyName={accessKeyName};SharedAccessKey={accessKey};EntityPath={eventHubName}"




ehConf = {}

ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
# ehConf["eventhubs.startingPosition"] = -1


df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

selected = df.withColumn("body", df["body"].cast("string"))
splited_df = selected.select(F.split(df["body"], "\\|"))

rename_col = splited_df.withColumn("split_data", splited_df)


parsed_df = rename_col.selectExpr("split_data[0] as timestamp", "split_data[1] as log_level", "split_data[2] as request_id", "split_data[3] as session_id", "split_data[4] as user_id", "split_data[5] as action", "split_data[6] as http_method", "split_data[7] as url", "split_data[8] as referrer_url", "split_data[9] as ip_address", "split_data[10] as user_agent", "split_data[11] as response_time", "split_data[12] as product_id", "split_data[13] as cart_size", "split_data[14] as checkout_status", "split_data[15] as token", "split_data[16] as auth_method", "split_data[17] as auth_level", "split_data[18] as correlation_id", "split_data[19] as server_ip", "split_data[20] as port", "split_data[21] as protocol", "split_data[22] as status_and_detail")

display(parsed_df)
