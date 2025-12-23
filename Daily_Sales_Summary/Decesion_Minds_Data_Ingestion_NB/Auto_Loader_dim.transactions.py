# Databricks notebook source
# Importing all required libraries

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import *

# COMMAND ----------

# Defining all paths
source_path = "abfss://landing@dailysalesdm1.dfs.core.windows.net/daily_sales/transactions/"
checkpoint_path = "abfss://bronze@dailysalesdm1.dfs.core.windows.net/unity_catalog_auto_loader_bronze/checkpoints/daily_sales/transactions/bronze_auto_loader_checkpoint"
schema_location = "abfss://bronze@dailysalesdm1.dfs.core.windows.net/unity_catalog_auto_loader_bronze/schema_location/daily_sales/transactions/bronze/"
target_table = "daily_sales_ct.bronze.dim_transactions"

# Configure Auto Loader to read the stream and evolve the schema
df_raw = (
  spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("multiline", "true")
  .option("cloudFiles.maxFilesPerTrigger", "1")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaLocation", schema_location)
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
  .load(source_path)
)

df_col = df_raw.withColumn("ingestion_timestamp", current_timestamp())

# Write to Unity Catalog managed table with availableNow trigger
query = (
  df_col.writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .trigger(availableNow=True)
  .toTable(target_table)
)

query.awaitTermination()