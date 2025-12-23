# Databricks notebook source
# importing python libraries

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

# Define the orders streaming table using a decorator
@dlt.create_streaming_table(
  name="daily_sales_ct_dlt.silver.dim_transactions",
  comment="SCD Type 2 table for tracking dimension changes"
)
def daily_sales_ct_dlt.silver.dim_transactions():
    # The table is created by the flow, no need to return a DataFrame here
    pass

# Apply the SCD Type 2 logic
dlt.create_auto_cdc_flow(
    target = "daily_sales_ct_dlt.silver.dim_transactions",
    source = "daily_sales_ct_dlt.bronze.dim_transactions", 
    keys = ["order_id"], 
    sequence_by = col("date"),
    stored_as_scd_type = "2", 
    track_history_except_column_list = ["_rescued_data", "date"] # Columns to ignore for change detection
)

# COMMAND ----------

# Define the transactions streaming table using a decorator
@dlt.create_streaming_table(
  name="daily_sales_ct_dlt.silver.dim_transactions",
  comment="SCD Type 2 table for tracking dimension changes"
)
def daily_sales_ct_dlt.silver.dim_transactions():
    # The table is created by the flow, no need to return a DataFrame here
    pass

# Apply the SCD Type 2 logic
dlt.create_auto_cdc_flow(
    target = "daily_sales_ct_dlt.silver.dim_transactions",
    source = "daily_sales_ct_dlt.bronze.dim_transactions", 
    keys = ["order_id"], 
    sequence_by = col("date"),
    stored_as_scd_type = "2", 
    track_history_except_column_list = ["", ""] # Columns to ignore for change detection
)

# COMMAND ----------

# Define the transactions streaming table using a decorator
@dlt.create_streaming_table(
  name="daily_sales_ct_dlt.silver.dim_transactions",
  comment="SCD Type 2 table for tracking dimension changes"
)
def daily_sales_ct_dlt.silver.dim_transactions():
    # The table is created by the flow, no need to return a DataFrame here
    pass

# Apply the SCD Type 2 logic
dlt.create_auto_cdc_flow(
    target = "daily_sales_ct_dlt.silver.dim_transactions",
    source = "daily_sales_ct_dlt.bronze.dim_transactions", 
    keys = ["order_id"], 
    sequence_by = col("date"),
    stored_as_scd_type = "2", 
    track_history_except_column_list = ["", ""] # Columns to ignore for change detection
)