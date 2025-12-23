# Databricks notebook source
#importing libraries

from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import date_format


# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating widget for catalog
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "daily_sales_ct"

# COMMAND ----------

# specifying catalog

catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# creating a Dataframe with Raw Layer.

df_bronze_trans = spark.sql(f" select * from {catalog}.bronze.dim_transactions")

# COMMAND ----------

# Standardize column names to snake case.

df_cleaned = (
    df_bronze_trans
    .withColumnRenamed("TransactionID", "transaction_id")
    .withColumnRenamed("Country", "country")
    .withColumnRenamed("PaymentMethod", "payment_method")
    .withColumnRenamed("TransactionDate", "transaction_date")
)

# COMMAND ----------

# performing data deduplication using row_number function.

window_spec = Window.partitionBy("transaction_id").orderBy(col("transaction_date").desc())

df_deduped = df_cleaned.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") == 1).drop("row_number")

# COMMAND ----------

# applying filter on transaction_id and converting transaction_date to date format
df_filter = (
    df_deduped
    .filter(col("transaction_id").isNotNull())
    .withColumn(
        "transaction_date",
        date_format(to_date(col("transaction_date"), "dd-MM-yyyy"), "yyyy-MM-dd")
    )
)

# COMMAND ----------

# performing delta merge on silver.dim_transactions

df_final = (
    df_filter
    .withColumn("run_date_time", current_timestamp())
)

target_table = DeltaTable.forName(
    spark,
    f"{catalog}.silver.dim_transactions"
)

target_table.alias("target").merge(
    df_final.alias("source"),
    "target.transaction_id = source.transaction_id"
).whenMatchedUpdate(
    set={
        "transaction_id": "source.transaction_id",
        "country": "source.country",
        "payment_method": "source.payment_method",
        "transaction_date": "source.transaction_date",
        "run_date_time": "source.run_date_time"
      
    }
).whenNotMatchedInsert(
    values={
        "transaction_id": "source.transaction_id",
        "country": "source.country",
        "payment_method": "source.payment_method",
        "transaction_date": "source.transaction_date",
        "run_date_time": "source.run_date_time"       
    }
).whenNotMatchedBySourceDelete().execute()