# Databricks notebook source
# MAGIC %md
# MAGIC #loading csv files to DeltaLake tables 

# COMMAND ----------

#importing libraries

from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating widgets for catalog
# MAGIC
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "daily_sales_ct"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --creating widget bronze_location
# MAGIC
# MAGIC CREATE WIDGET TEXT bronze_location DEFAULT "abfss://bronze@dailysalesdm1.dfs.core.windows.net/"

# COMMAND ----------

# specifying catalog and bronze location

catalog = dbutils.widgets.get("catalog")
bronze_location = dbutils.widgets.get("bronze_location")
orders_data_location = f"{bronze_location}/daily_sales"
customers_data_location = f"{bronze_location}/daily_sales"
transactions_data_location = f"{bronze_location}/daily_sales"

# COMMAND ----------

# reading orders data from csv file and load to delta table

orders_csv_schema = StructType(
    [
        StructField("Order Id",IntegerType(),True),
        StructField("Date", DateType(), True),
        StructField("Product", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Price", FloatType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Revenue", IntegerType(), True)
    ]
)

orders_file_path = "abfss://landing@dailysalesdm1.dfs.core.windows.net/daily_sales/orders.csv"

delta_table_location = f"{orders_data_location}/dim_orders"

df_csv_orders = spark.read.csv(orders_file_path, header=True, schema=orders_csv_schema)

# COMMAND ----------

# reading customers data from csv files and load to delta table

customers_csv_schema = StructType(
    [
        StructField("Customer ID",IntegerType(),True),
        StructField("Customer_Name", DateType(), True),
        StructField("Age", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Education", FloatType(), True),
        StructField("Purchased", IntegerType(), True)
    ]
)

customers_file_path = "abfss://landing@dailysalesdm1.dfs.core.windows.net/daily_sales/customers.csv"

delta_table_location = f"{customers_data_location}/dim_customers"

df_csv_customers = spark.read.csv(customers_file_path, header=True, schema=customers_csv_schema)

# COMMAND ----------

# reading transactions data from csv files and load to delta table

transactions_csv_schema = StructType(
    [
        StructField("Transaction ID",IntegerType(),True),
        StructField("Country", DateType(), True),
        StructField("Payment_Method", StringType(), True),
        StructField("Transaction_Date", StringType(), True)
    ]
)

transactions_file_path = "abfss://landing@dailysalesdm1.dfs.core.windows.net/sample_project/transactions.csv"

delta_table_location = f"{transactions_data_location}/dim_transactions"

df_csv_transactions = spark.read.csv(transactions_file_path, header=True, schema=transactions_csv_schema)

# COMMAND ----------

# Performing Delta Merge on orders Data

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

df_final = df_csv_orders.withColumn("run_date_time", current_timestamp())

target_table = DeltaTable.forName(spark, f"{catalog}.bronze.dim_orders")

target_table.alias("target").merge(
    df_final.alias("source"),
    "target.Order Id = source.Order Id"
).whenMatchedUpdate(
    set={
        "Order Id": "source.Order Id",
        "Date": "source.Date",
        "Product": "source.Product",
        "Category": "source.Category",
        "Price": "source.Price",
        "Quantity": "source.Quantity",
        "Revenue": "source.Revenue",
        "run_date_time": "source.run_date_time"

    }
).whenNotMatchedInsert(
    values={
        "Order Id": "source.Order Id",
        "Date": "source.Date",
        "Product": "source.Product",
        "Category": "source.Category",
        "Price": "source.Price",
        "Quantity": "source.Quantity",
        "Revenue": "source.Revenue",
        "run_date_time": "source.run_date_time"
    }
).execute()

# COMMAND ----------

# Performing Delta Merge on Customers Data

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

df_final = df_csv_customers.withColumn("run_date_time", current_timestamp())

target_table = DeltaTable.forName(spark, f"{catalog}.bronze.dim_customers")

target_table.alias("target").merge(
    df_final.alias("source"),
    "target.Customer ID = source.Customer ID"
).whenMatchedUpdate(
    set={
        "Customer ID": "source.Customer ID",
        "Customer_Name": "source.Customer_Name",
        "Age": "source.Age",
        "Gender": "source.Gender",
        "Education": "source.Education",
        "Purchased": "source.Purchased",
        "run_date_time": "source.run_date_time"
    }
).whenNotMatchedInsert(
    values={
        "Customer ID": "source.Customer ID",
        "Customer_Name": "source.Customer_Name",
        "Age": "source.Age",
        "Gender": "source.Gender",
        "Education": "source.Education",
        "Purchased": "source.Purchased",
        "run_date_time": "source.run_date_time"
    }
).execute()

# COMMAND ----------

# Performing Delta Merge on Transactions Data

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

df_final = df_csv_transactions.withColumn("run_date_time", current_timestamp())

target_table = DeltaTable.forName(spark, f"{catalog}.bronze.dim_transactions")

target_table.alias("target").merge(
    df_final.alias("source"),
    "target.Transaction ID = source.Transaction ID"
).whenMatchedUpdate(
    set={
        "Transaction ID": "source.Transaction ID",
        "Country": "source.Country",
        "Payment_Method": "source.Payment_Method",
        "Transaction_Date": "source.Transaction_Date",
        "run_date_time": "source.run_date_time"
    }
).whenNotMatchedInsert(
    values={
        "Transaction ID": "source.Transaction ID",
        "Country": "source.Country",
        "Payment_Method": "source.Payment_Method",
        "Transaction_Date": "source.Transaction_Date",
        "run_date_time": "source.run_date_time"
    }
).execute()