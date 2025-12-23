import dlt
from pyspark.sql.functions import col, monotonically_increasing_id, current_timestamp

@dlt.table(
    name="daily_sales_ct_dlt.silver_mv.dim_customers",
    comment="Materialized view for cleaned and deduplicated customers data"
)

def silver_customers_mv():
    bronze_df = dlt.read("daily_sales_ct_dlt.bronze.dim_customers_dlt")

    renamed_df = (
        bronze_df
        .withColumnRenamed("CustomerID", "customer_id")
        .withColumnRenamed("CustomerName", "customer_name")
        .withColumnRenamed("Age", "age")
        .withColumnRenamed("Gender", "gender")
        .withColumnRenamed("Education", "education")
        .withColumnRenamed("Purchased", "purchased")
        .withColumnRenamed("CreatedDate", "created_date")
        .withColumn("run_date_time", current_timestamp())
    )

    type_casted_df = (
        renamed_df
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("customer_name", col("customer_name").cast("string"))
        .withColumn("age", col("age").cast("int"))
        .withColumn("gender", col("gender").cast("string"))
        .withColumn("education", col("education").cast("string"))
        .withColumn("purchased", col("purchased").cast("string"))
        .withColumn("created_date", col("created_date").cast("date"))
    )

    return (
        type_casted_df
        .withColumn("surrogate_key", monotonically_increasing_id())
        .drop("[")
        .drop("customer_id is null")
        .drop("_rescued_data")
    )