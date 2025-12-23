# Databricks notebook source
# MAGIC %sql
# MAGIC -- creating widget for catalog
# MAGIC -- creating widget for silver location
# MAGIC
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "daily_sales_ct";
# MAGIC CREATE WIDGET TEXT silver_location DEFAULT ""abfss://silver@dailysalesdm1.dfs.core.windows.net/""

# COMMAND ----------

# importing all python libraries
# importing delta module

from delta.tables import DeltaTable
from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, StringType, FloatType, TimestampType


# COMMAND ----------

# specifying catalog

catalog = dbutils.widgets.get("catalog")

silver_location = dbutils.widgets.get("silver_location")

silver_delta_path = f"{silver_location}/daily_sales/dim_orders"

# COMMAND ----------

# creating a delta table for dim_orders

df_silver_dim_orders = (
    DeltaTable.createIfNotExists(spark)
    .tableName(f"{catalog}.silver.dim_orders")
    .addColumn("order_id", "int", nullable=False)
    .addColumn("order_date", "date", nullable=True)
    .addColumn("product", "string", nullable=True)
    .addColumn("category", "string", nullable=True)
    .addColumn("price", "float", nullable=True)
    .addColumn("quantity", "int", nullable=True)
    .addColumn("revenue", "float", nullable=True)
    .addColumn("run_date_time", "timestamp", nullable=True)
    .location(silver_delta_path)
    .execute()
)