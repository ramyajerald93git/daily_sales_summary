# Databricks notebook source
# Importing Libraries

import dlt
from pyspark.sql import functions as F

# COMMAND ----------



@dlt.materialized_view(
    name="daily_sales_report_gold",
    comment="Daily aggregated sales, customers, and transactions report for analysis",
    # You can add table properties for optimization like ZORDER
    table_properties={"pipelines.autoOptimize.zOrderCols": "report_date"}
)
def daily_sales_report_gold():
    # Read from your silver layer tables (ensure these tables exist and are updated)
    sales_df = spark.read.table("LIVE.sales_silver")
    customers_df = spark.read.table("LIVE.customers_silver")
    transactions_df = spark.read.table("LIVE.transactions_silver")

    # Aggregate sales and transactions data by date
    daily_agg_sales = sales_df.groupBy("sale_date").agg(
        F.sum("sale_amount").alias("total_daily_sales"),
        F.countDistinct("customer_id").alias("unique_customers_count"),
        F.count("transaction_id").alias("total_transactions")
    ).withColumnRenamed("sale_date", "report_date")

    # Optional: Join with relevant customer or product dimensions if needed for detailed reporting
    # For a simple daily summary, aggregation is often enough.

    # Apply data quality expectations (optional but recommended for Gold layer)
    return daily_agg_sales.filter(F.col("total_daily_sales") >= 0).with_expectations({
        "valid_total_sales": "total_daily_sales IS NOT NULL AND total_daily_sales >= 0",
        "valid_transactions": "total_transactions > 0"
    })