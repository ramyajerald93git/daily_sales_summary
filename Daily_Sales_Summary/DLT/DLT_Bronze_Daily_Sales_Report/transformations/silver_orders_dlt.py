import dlt
from pyspark.sql.functions import col, monotonically_increasing_id, current_timestamp

@dlt.table(
    name="daily_sales_ct_dlt.silver_mv.dim_orders",
    comment="Materialized view for cleaned and deduplicated orders data"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL")
def silver_orders_mv():
    bronze_df = dlt.read("daily_sales_ct_dlt.bronze.dim_orders_dlt")

    renamed_df = (
        bronze_df
        .withColumnRenamed("OrderId", "order_id")
        .withColumnRenamed("Date", "order_date")
        .withColumnRenamed("Product", "product")
        .withColumnRenamed("Category", "category")
        .withColumnRenamed("Price", "price")
        .withColumnRenamed("Quantity", "quantity")
        .withColumnRenamed("Revenue", "revenue")
        .withColumn("run_date_time", current_timestamp())
    )

    type_casted_df = (
        renamed_df
        .withColumn("order_id", col("order_id").cast("int"))
        .withColumn("product", col("product").cast("string"))
        .withColumn("category", col("category").cast("string"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("revenue", col("revenue").cast("double"))
    )

    # Deduplicate using dropDuplicates with event-time column
    deduped_df = type_casted_df.dropDuplicates(["order_id", "order_date"])

    return (
        deduped_df
        .withColumn("surrogate_key", monotonically_increasing_id())
        .drop("_rescued_data")
    )