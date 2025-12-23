# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver layer Transformations  

# COMMAND ----------

#importing libraries

from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating widgets for catalog
# MAGIC
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "daily_sales_ct"

# COMMAND ----------

# specifying catalog

catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# creating a Dataframe with Raw Layer.

df_bronze = spark.sql(f" select * from {catalog}.bronze.dim_orders")

# COMMAND ----------

# data standardization - convert column names to snake case.

df_cleaned = df_bronze.withColumnRenamed("OrderId", "order_id") \
                      .withColumnRenamed("Date", "order_date") \
                      .withColumnRenamed("Product", "product") \
                      .withColumnRenamed("Category", "category") \
                      .withColumnRenamed("Price", "price") \
                      .withColumnRenamed("Quantity", "quantity") \
                      .withColumnRenamed("Revenue", "revenue")

# COMMAND ----------

# performing data deduplication using row_number function.

window_spec = Window.partitionBy("order_id").orderBy(col("order_date").desc())

df_deduped = df_cleaned.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") == 1).drop("row_number")

# COMMAND ----------

# applying filter on order_no to remove null values

df_filtered = df_deduped.filter(col("order_id").isNotNull())

# COMMAND ----------

# Perform Delta Merge for silver.dim_orders

from pyspark.sql.functions import to_date, current_timestamp, col

df_final = (
    df_filtered
    .withColumn("run_date_time", current_timestamp())
)

target_table = DeltaTable.forName(
    spark,
    f"{catalog}.silver.dim_orders"
)

target_table.alias("target").merge(
    df_final.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdate(
    set={
        "order_id": col("source.order_id"),
        "order_date": col("source.order_date"),
        "product": col("source.product"),
        "category": col("source.category"),
        "price": col("source.price"),
        "quantity": col("source.quantity"),
        "revenue": col("source.revenue"),
        "run_date_time": col("source.run_date_time")
    }
).whenNotMatchedInsert(
    values={
        "order_id": col("source.order_id"),
        "order_date": col("source.order_date"),
        "product": col("source.product"),
        "category": col("source.category"),
        "price": col("source.price"),
        "quantity": col("source.quantity"),
        "revenue": col("source.revenue"),
        "run_date_time": col("source.run_date_time")
    }
).whenNotMatchedBySourceDelete().execute()