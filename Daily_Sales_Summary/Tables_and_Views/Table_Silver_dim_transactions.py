# Databricks notebook source
# MAGIC %sql
# MAGIC -- creating widget for catalog
# MAGIC -- creating widget for silver location
# MAGIC
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "daily_sales_ct";
# MAGIC CREATE WIDGET TEXT silver_location DEFAULT "abfss://silver@dailysalesdm1.dfs.core.windows.net/"

# COMMAND ----------

# importing all python lybraries
# importing delta module

from delta.tables import DeltaTable
from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, StringType, FloatType, TimestampType


# COMMAND ----------

# specifying catalog(getting)

catalog = dbutils.widgets.get("catalog")

silver_location = dbutils.widgets.get("silver_location")

silver_delta_path = f"{silver_location}/daily_sales/dim_transactions"

# COMMAND ----------

# creating delta table for transcations

df_silver_dim_transactions = (
    DeltaTable.createIfNotExists(spark)
    .tableName(f"{catalog}.silver.dim_transactions")
    .addColumn("transaction_id", "string", nullable=False)
    .addColumn("country", "string", nullable=True)
    .addColumn("payment_method", "string", nullable=True)
    .addColumn("transaction_date", "date", nullable=True)    
    .addColumn("run_date_time", "timestamp", nullable=True)
    .location(silver_delta_path)
    .execute()
)