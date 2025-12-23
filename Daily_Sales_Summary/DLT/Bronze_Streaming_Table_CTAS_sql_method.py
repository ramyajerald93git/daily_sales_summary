# Databricks notebook source
# MAGIC %skip
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE daily_sales_ct_dlt.bronze.dim_orders_dlt
# MAGIC COMMENT "Raw orders data ingested from ADLS Gen2 containers (landing)"
# MAGIC AS SELECT * FROM STREAM read_files(
# MAGIC   "abfss://landing@storageaccsp.dfs.core.windows.net/daily_sales_report/",
# MAGIC   format => "json",
# MAGIC   schemaLocation => "abfss://landing@storageaccsp.dfs.core.windows.net/daily_sales_report/checkpoints/schema/dim_sales_dlt/",
# MAGIC   rescuedDataColumn => "_rescued_data"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE daily_sales_ct_dlt.bronze.dim_orders_dlt
# MAGIC   COMMENT "Raw orders data ingested from ADLS Gen2 containers (landing)" AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   STREAM read_files(
# MAGIC     "/Volumes/daily_sales_ct/bronze/raw_data/orders/orders.csv",
# MAGIC     format => "csv",
# MAGIC     schemaLocation => "/Volumes/daily_sales_ct_dlt/bronze/bronze_dlt/schema/orders/",
# MAGIC     rescuedDataColumn => "_rescued_data"
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE daily_sales_ct_dlt.bronze.dim_customers_dlt
# MAGIC   COMMENT "Raw customers data ingested from ADLS Gen2 containers (landing)" AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   STREAM read_files(
# MAGIC     "/Volumes/daily_sales_ct/bronze/raw_data/customers/customers.json",
# MAGIC     format => "json",
# MAGIC     schemaLocation => "/Volumes/daily_sales_ct_dlt/bronze/bronze_dlt/schema/customers/",
# MAGIC     rescuedDataColumn => "_rescued_data"
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE daily_sales_ct_dlt.bronze.dim_transactions_dlt
# MAGIC   COMMENT "Raw customers data ingested from ADLS Gen2 containers (landing)" AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   STREAM read_files(
# MAGIC     "/Volumes/daily_sales_ct/bronze/raw_data/transactions/transactions.json",
# MAGIC     format => "json",
# MAGIC     schemaLocation => "/Volumes/daily_sales_ct_dlt/bronze/bronze_dlt/schema/transactions/",
# MAGIC     rescuedDataColumn => "_rescued_data"
# MAGIC   )