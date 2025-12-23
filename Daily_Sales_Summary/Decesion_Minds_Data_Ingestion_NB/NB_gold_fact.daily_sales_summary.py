# Databricks notebook source
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

# specying catalog

catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# creating Dataframes from all the silver tables

df_silver_orders = spark.read.table(f"{catalog}.silver.dim_orders")

df_silver_customers = spark.read.table(f"{catalog}.silver.dim_customers")

df_silver_transactions = spark.read.table(f"{catalog}.silver.dim_transactions") 

# COMMAND ----------

#  joining dim_orders, dim_customers and dim_transactions

df_join = (
    df_silver_orders
    .join(
        df_silver_customers,
        df_silver_orders.order_id == df_silver_customers.customer_id,
        "inner"
    )
    .join(
        df_silver_transactions,
        df_silver_customers.customer_id == df_silver_transactions.transaction_id,
        "inner"
    )
)

# COMMAND ----------

# Implementing aggregation operation for daily_sales_summary 

df_aggr_daily_sales_report = (
    df_join
    .groupBy("customer_id", "customer_name", "age", "gender", "product", "education", "category", "payment_method", "transaction_date", "country")
    .agg(
        first("transaction_date").alias("sale_date"),
        sum("revenue").alias("total_sales_amount"),
        sum("quantity").alias("total_units_sold"),
        countDistinct("order_id").alias("unique_orders"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy("customer_name", ascending=True)
)

# COMMAND ----------

# perform delta merge operation to update the gold table  
                               
df_final = (
    df_aggr_daily_sales_report
    .withColumn("run_date_time", current_timestamp())
)

target_table = DeltaTable.forName(
    spark,
    f"{catalog}.gold.fact_sales_summary"
)

target_table.alias("target").merge(
    df_final.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdate(
    set={
        "customer_id": "source.customer_id",
        "customer_name": "source.customer_name",
        "age": "source.age",
        "gender": "source.gender",
        "product": "source.product",
        "education": "source.education",
        "category": "source.category",
        "payment_method": "source.payment_method",
        "order_date": "source.order_date",
        "country": "source.country",
        "sale_date": "source.sale_date",
        "total_sales_amount": "source.total_sales_amount",
        "total_units_sold": "source.total_units_sold",
        "unique_orders": "source.unique_orders",
        "unique_customers": "source.unique_customers"
    }
).whenNotMatchedInsert(
    values={
            "customer_id": "source.customer_id",
            "customer_name": "source.customer_name",
            "age": "source.age",
            "gender": "source.gender",
            "product": "source.product",
            "education": "source.education",
            "category": "source.category",
            "payment_method": "source.payment_method",
            "order_date": "source.order_date",
            "country": "source.country",
            "sale_date": "source.sale_date",
            "total_sales_amount": "source.total_sales_amount",
            "total_units_sold": "source.total_units_sold",
            "unique_orders": "source.unique_orders",
            "unique_customers": "source.unique_customers"
    }
).whenNotMatchedBySourceDelete().execute()