# Databricks notebook source
#importing libraries

from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating widget for catalog
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "daily_sales_ct"

# COMMAND ----------

# specifying catalog

catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# creating a Dataframe with Raw Layer.

df_bronze_sales = spark.sql(f" select * from {catalog}.bronze.dim_customers")

# COMMAND ----------

# Standardize column names to snake case.
df_cleaned = (
    df_bronze_sales
    .withColumnRenamed("CustomerID", "customer_id")
    .withColumnRenamed("CustomerName", "customer_name")
    .withColumnRenamed("Age", "age")
    .withColumnRenamed("Gender", "gender")
    .withColumnRenamed("Education", "education")
    .withColumnRenamed("Purchased", "purchased")
    .withColumnRenamed("CreatedDate", "created_date")
  )

# COMMAND ----------

# performing data deduplication using row_number function.

window_spec = Window.partitionBy("customer_id").orderBy(col("created_date").desc())

df_deduped = df_cleaned.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") == 1).drop("row_number")

# COMMAND ----------

df_date_corr = df_deduped.withColumn(
    "created_date",
    date_format(
        to_date(col("created_date"), "dd-MM-yyyy"),
        "yyyy-MM-dd"
    )
)

# COMMAND ----------

# removing null values from customer_id column.

df_filter = df_date_corr.filter(col("customer_id").isNotNull())

# COMMAND ----------

# performing delta merge on silver.dim_customers

df_final = (
    df_filter
    .withColumn("run_date_time", current_timestamp())
)

target_table = DeltaTable.forName(
    spark,
    f"{catalog}.silver.dim_customers"
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
        "education": "source.education",
        "purchased": "source.purchased",
        "created_date": "source.created_date",
        "run_date_time": "source.run_date_time"
    }
).whenNotMatchedInsert(
    values={
        "customer_id": "source.customer_id",
        "customer_name": "source.customer_name",
        "age": "source.age",
        "gender": "source.gender",
        "education": "source.education",
        "purchased": "source.purchased",
        "created_date": "source.created_date",
        "run_date_time": "source.run_date_time"
    }
).whenNotMatchedBySourceDelete().execute()