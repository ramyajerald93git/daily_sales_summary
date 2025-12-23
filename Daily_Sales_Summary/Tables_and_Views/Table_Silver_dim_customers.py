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

# specifying catalog

catalog = dbutils.widgets.get("catalog")

silver_location = dbutils.widgets.get("silver_location")

silver_delta_path = f"{silver_location}/daily_sales/dim_customers"

# COMMAND ----------

# creating delta table for dim_sales

df_silver_dim_sales = (
    DeltaTable.createIfNotExists(spark)
    .tableName(f"{catalog}.silver.dim_customers")
    .addColumn("customer_id","int", nullable=False)
    .addColumn("customer_name","string", nullable=True)
    .addColumn("age","int", nullable=True)
    .addColumn("gender","string", nullable=True)
    .addColumn("education","string", nullable=True)
    .addColumn("purchased","string", nullable=True)
    .addColumn("created_date","date", nullable=True)
    .addColumn("run_date_time","timestamp", nullable=True)
    .location(silver_delta_path)
    .execute()
)