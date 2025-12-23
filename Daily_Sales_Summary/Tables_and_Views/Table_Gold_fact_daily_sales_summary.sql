-- Databricks notebook source
-- creating widget for catalog
-- creating widget for gold location

CREATE WIDGET TEXT catalog DEFAULT "daily_sales_ct";
CREATE WIDGET TEXT silver_location DEFAULT "abfss://gold@dailysalesdm1.dfs.core.windows.net/"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # importing all python libraries
-- MAGIC # importing delta module
-- MAGIC
-- MAGIC from delta.tables import DeltaTable
-- MAGIC from pyspark.sql.functions import col, when, lit, row_number
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.types import IntegerType, DateType, StringType, FloatType, TimestampType
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # specifying catalog
-- MAGIC
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC
-- MAGIC gold_location = dbutils.widgets.get("gold_location")
-- MAGIC
-- MAGIC gold_delta_path = f"{gold_location}/daily_sales/fact_daily_sales_summary"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # creating a delta table for fact_daily_sales_summary
-- MAGIC
-- MAGIC df_gold_fact_daily_sales_summary = (
-- MAGIC     DeltaTable.createIfNotExists(spark)
-- MAGIC     .tableName(f"{catalog}.gold.fact_daily_sales_summary")
-- MAGIC     .addColumn("customer_id", "int", nullable=False)
-- MAGIC     .addColumn("customer_name", "string", nullable=True)
-- MAGIC     .addColumn("age", "int", nullable=True)
-- MAGIC     .addColumn("gender", "string", nullable=True)
-- MAGIC     .addColumn("product", "string", nullable=True)
-- MAGIC     .addColumn("education", "string", nullable=True)
-- MAGIC     .addColumn("category", "string", nullable=True)
-- MAGIC     .addColumn("payment_method", "string", nullable=True)
-- MAGIC     .addColumn("order_date", "date", nullable=True)
-- MAGIC     .addColumn("country", "string", nullable=True)
-- MAGIC     .addColumn("sale_date", "date", nullable=True)
-- MAGIC     .addColumn("total_sales_amount", "double", nullable=True)
-- MAGIC     .addColumn("total_units_sold", "bigint", nullable=True)
-- MAGIC     .addColumn("unique_orders", "bigint", nullable=True)
-- MAGIC     .addColumn("unique_customers", "bigint", nullable=True)
-- MAGIC     .addColumn("run_date_time", "timestamp", nullable=True)
-- MAGIC     .location(gold_delta_path)
-- MAGIC     .execute()
-- MAGIC )