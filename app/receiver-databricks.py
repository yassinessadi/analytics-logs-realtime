# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC import `libs` & `models`:
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, expr,concat_ws,regexp_extract,regexp_replace,when
from pyspark.sql.functions import col, unbase64,date_format

# COMMAND ----------

# MAGIC %md
# MAGIC `variables` & `expressions`

# COMMAND ----------

#-------------------------------------------------------#
#  this credentials will expire soon replace with yours #
#-------------------------------------------------------#
namespace = "regulargazelleseventhub"
accessKeyName = "RootManageSharedAccessKey"
accessKey = "s4ps7/DF/iv+Vy/gn699+Gaya2fzyfmCL+AEhBu86Z0="
eventHubName = "handler-logs"
starting_position = -1

connection_string = f"Endpoint=sb://{namespace}.servicebus.windows.net/;SharedAccessKeyName={accessKeyName};SharedAccessKey={accessKey};EntityPath={eventHubName}"

#------------------------------------------------------------------------------------------------------------#
# event hub config to read more check -----------------------------------------------------------------------#
# https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md   #
#------------------------------------------------------------------------------------------------------------#

ehConf = {
    "eventhubs.consumerGroup": "$Default",
}
#---------------------------------------------------------------------------------------------------------#
# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.# 
#---------------------------------------------------------------------------------------------------------#

ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)


# COMMAND ----------

#-----------------------------------------#
# init spark session andcreate hive table #
#-----------------------------------------#
spark = SparkSession.builder.appName("EventHubslogshandler").getOrCreate()
database_name = 'HAEE_db'

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

spark.sql(f"USE {database_name}")

#----------------------#
# Read from Event Hubs #
#----------------------#
raw_df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

#------------------------------------------------#
# Convert the value column from binary to string #
#------------------------------------------------#
raw_df = raw_df.withColumn("value", raw_df["body"].cast("string"))

#----------------------------------------#
# Split the data using the "|" delimiter #
#----------------------------------------#

split_columns = split(raw_df["value"], "\\|")
split_df = raw_df.withColumn("split_data", split_columns)
#---------------------------------------------------------#
# Select the split columns and apply the defined schema   #
#---------------------------------------------------------#
parsed_df = split_df.selectExpr("split_data[0] as timestamp", "split_data[1] as log_level", "split_data[2] as request_id", "split_data[3] as session_id", "split_data[4] as user_id", "split_data[5] as action", "split_data[6] as http_method", "split_data[7] as url", "split_data[8] as referrer_url", "split_data[9] as ip_address", "split_data[10] as user_agent", "split_data[11] as response_time", "split_data[12] as product_id", "split_data[13] as cart_size", "split_data[14] as checkout_status", "split_data[15] as token", "split_data[16] as auth_method", "split_data[17] as auth_level", "split_data[18] as correlation_id", "split_data[19] as server_ip", "split_data[20] as port", "split_data[21] as protocol", "split_data[22] as status", "split_data[23] as detail")

# ------------------------#
# realtime transformation #
#-------------------------#

pattern = r'(\d+\.\d+)'

select_trasnformed_df = parsed_df.select(
    date_format(split(col("timestamp"),"\"").getItem(1),"yyyy-MM-dd HH:mm:ss").alias("timestamp"),
    split(col("ip_address"), ":").getItem(1).alias("ip_address"),
    split(col("server_ip"), ":").getItem(1).alias("server_ip"),
    col("session_id").alias("session_id"),
    col("http_method").alias("http_method"),
    regexp_replace(col("url").alias("url"),"/","").alias("page_name"),
    split(col("Referrer_url"), "\\.").getItem(1).alias("Referrer_url"),
    split(col("cart_size"), ":").getItem(1).alias("cart_size"),
    split(col("checkout_status"), ":").getItem(1).alias("checkout_status"),
    split(col("product_id"), ":").getItem(1).alias("product_id"),
    col("request_id").alias("request_id"),
    col("log_level").alias("log_level"),
    regexp_extract(col("response_time"), pattern,0).alias("response_time"),
    split(col("correlation_id"), ":").getItem(1).alias("correlation_id"),
    split(col("auth_method"), ":").getItem(1).alias("auth_method"),
    split(col("auth_level"), ":").getItem(1).alias("auth_level"),
    split(col("Token"), ":").getItem(1).alias("Token"),
    split(col("port"), ":").getItem(1).alias("port"),
    split(col("status"), " ").getItem(1).alias("status"),
    split(col("protocol"), ":").getItem(1).alias("protocol"),
    split(col("detail"), ":").getItem(1).alias("detail"),
    split(col("user_agent"), ":").getItem(1).alias("user_agent"),
    col("action").alias("action"),
)


select_trasnformed_df = select_trasnformed_df.withColumn("Referrer_url", \
    when(col("Referrer_url").isNull(),"Direct Entry") \
    .otherwise(col("Referrer_url"))) \

#-------------------------------------------------------------------#
# remove the data contains a null value (timestamp & response_time) #
#-------------------------------------------------------------------#
select_trasnformed_df = select_trasnformed_df.filter("timestamp IS NOT NULL and response_time IS NOT NULL")

#------------------------------------------------#
# Write the parsed DataFrame to the console sink #
#------------------------------------------------#

# select_trasnformed_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()



#------------------------------------#
#        write streams in hive       #
#------------------------------------#

hive_table_name = 'haee_logs'
table_location = '/mnt/haee_workspace/logs'
select_trasnformed_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/mnt/checkpoints/dir") \
  .toTable("haee_logs")

#--------------------#
# display the output #
#--------------------#
display(select_trasnformed_df)
   

# COMMAND ----------


