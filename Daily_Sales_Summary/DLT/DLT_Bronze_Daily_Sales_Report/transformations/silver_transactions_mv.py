import dlt
from pyspark.sql.functions import col, monotonically_increasing_id, current_timestamp

@dlt.table(
    name="daily_sales_ct_dlt.silver_mv.dim_transactions",
    comment="Materialized view for cleaned and deduplicated transaction data"
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_transaction_date", "transaction_date IS NOT NULL")
def silver_transactions_mv():
    bronze_df = dlt.read("daily_sales_ct_dlt.bronze.dim_transactions_dlt")

    renamed_df = (
        bronze_df
        .withColumnRenamed("TransactionID", "transaction_id")
        .withColumnRenamed("TransactionDate", "transaction_date")
        .withColumnRenamed("PaymentMethod", "payment_method")
        .withColumnRenamed("Country", "country")
        .withColumn("run_date_time", current_timestamp())
    )

    type_casted_df = (
        renamed_df
        .withColumn("transaction_id", col("transaction_id").cast("int"))
        .withColumn("transaction_date", col("transaction_date").cast("date"))
        .withColumn("payment_method", col("payment_method").cast("string"))
        .withColumn("country", col("country").cast("string"))
    )

    # Deduplicate using dropDuplicates with correct columns
    deduped_df = type_casted_df.dropDuplicates(["transaction_id", "transaction_date"])

    return (
        deduped_df
        .withColumn("surrogate_key", monotonically_increasing_id())
        .drop("[")
        .drop("_rescued_data")
    )