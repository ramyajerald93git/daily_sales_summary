# Databricks notebook source
# Importing Libraries

import dlt
from pyspark.sql.functions import col, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

# COMMAND ----------


@dlt.table(
    name="daily_sales_ct_dlt.silver_mv.dim_orders",
    comment="Materialized view for cleaned and deduplicated orders data"
)
def silver_orders():
    # Read as a stream from the bronze table
    bronze_df = dlt.read_stream("daily_sales_ct_dlt.bronze.dim_transactions")

    # 1. Apply data transformations - renaming, type casting, filtering
    transformed_df = bronze_df.withColumnRenamed("OrderId", "order_id") \
                              .withColumnRenamed("Date","order_date") \
                              .withColumnRenamed("Product","product") \
                              .withColumnRenamed("Category","product") \
                              .withColumn("signup_date", col("signup_date").cast("date")) \
                              .filter(col("order_id").isNotNull())

    # 2. Implement deduplication using a window function - row_number()
    # This approach keeps the latest record based on a 'timestamp' and primary key
    window_spec = Window.partitionBy("order_id").orderBy(col("date").desc())
    deduped_df = transformed_df.withColumn("row_num", row_number().over(window_spec)) \
                               .filter("row_num = 1") \
                               .drop("row_num")

    # 3. Adding data quality expectations
    # The 'expect_or_drop' directive drops records that violate the quality rule - NULL Check on Unique Identifier Key - 'customer_id' 
     # check the date format as a exception

    return deduped_df.expect_or_drop("valid_customer_id", "order_id IS NOT NULL") \
                     .expect_or_drop("valid_date_format", "order_date in 'yyyy-MM-dd'") \
                     .withColumn("surrogate key", monotonically_increasing_id()) \ 
                     .withColumn("ingest_timestamp", current_timestamp()) 

# COMMAND ----------


@dlt.table(
    name="daily_sales_ct_dlt.silver.dim_transactions",
    comment="Cleaned and deduplicated orders data in the silver layer"
)
def silver_orders():
    # Read as a stream from the bronze table
    bronze_df = dlt.read_stream("daily_sales_ct_dlt.bronze.dim_transactions")

    # 1. Apply data transformations - renaming, type casting, filtering
    transformed_df = bronze_df.withColumnRenamed("OrderId", "order_id") \
                              .withColumnRenamed("Date","order_date") \
                              .withColumnRenamed("Product","product") \
                              .withColumnRenamed("Category","product") \
                              .withColumn("signup_date", col("signup_date").cast("date")) \
                              .filter(col("order_id").isNotNull())

    # 2. Implement deduplication using a window function - row_number()
    # This approach keeps the latest record based on a 'timestamp' and primary key
    window_spec = Window.partitionBy("order_id").orderBy(col("date").desc())
    deduped_df = transformed_df.withColumn("row_num", row_number().over(window_spec)) \
                               .filter("row_num = 1") \
                               .drop("row_num")

    # 3. Adding data quality expectations
    # The 'expect_or_drop' directive drops records that violate the quality rule - NULL Check on Unique Identifier Key - 'customer_id' 
     # check the date format as a exception

    return deduped_df.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL") \
                     .expect_or_drop("valid_date_format", "order_date in 'yyyy-MM-dd'") \
                     .withColumn("surrogate key", monotonically_increasing_id()) \ 
                     .withColumn("ingest_timestamp", current_timestamp()) 

# COMMAND ----------


@dlt.table(
    name="daily_sales_ct_dlt.silver.dim_transactions",
    comment="Cleaned and deduplicated orders data in the silver layer"
)
def silver_orders():
    # Read as a stream from the bronze table
    bronze_df = dlt.read_stream("daily_sales_ct_dlt.bronze.dim_transactions")

    # 1. Apply data transformations - renaming, type casting, filtering
    transformed_df = bronze_df.withColumnRenamed("", "") \
                              .withColumnRenamed("","") \
                              .withColumnRenamed("","") \
                              .withColumnRenamed("","") \
                              .withColumn("", col("").cast("date")) \
                              .filter(col("transaction_id").isNotNull())

    # 2. Implement deduplication using a window function - row_number()
    # This approach keeps the latest record based on a 'timestamp' and primary key
    window_spec = Window.partitionBy("transaction_id").orderBy(col("transaction_date").desc())
    deduped_df = transformed_df.withColumn("row_num", row_number().over(window_spec)) \
                               .filter("row_num = 1") \
                               .drop("row_num")

    # 3. Adding data quality expectations
    # The 'expect_or_drop' directive drops records that violate the quality rule - NULL Check on Unique Identifier Key - 'customer_id' 
     # check the date format as a exception

    return deduped_df.expect_or_drop("valid_customer_id", "transaction_id IS NOT NULL") \
                     .expect_or_drop("valid_date_format", "transaction_date in 'yyyy-MM-dd'") \
                     .withColumn("surrogate key", monotonically_increasing_id()) \ 
                     .withColumn("ingest_timestamp", current_timestamp()) 