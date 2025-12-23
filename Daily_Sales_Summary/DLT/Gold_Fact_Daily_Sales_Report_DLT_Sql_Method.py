# Databricks notebook source
# MAGIC %sql
# MAGIC -- Define the gold layer table: daily_sales_report_gold
# MAGIC CREATE LIVE TABLE daily_sales_report_gold
# MAGIC COMMENT "Daily aggregated sales, customer, and transaction data for reporting using SQL."
# MAGIC AS
# MAGIC SELECT
# MAGIC     sale_date,
# MAGIC     SUM(amount) AS total_sales,
# MAGIC     COUNT(DISTINCT transaction_id) AS total_transactions,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers_per_day,
# MAGIC     CURRENT_DATE() AS report_date
# MAGIC FROM
# MAGIC     LIVE.sales_silver -- Use the 'LIVE.' schema prefix to reference other DLT tables
# MAGIC GROUP BY
# MAGIC     sale_date;