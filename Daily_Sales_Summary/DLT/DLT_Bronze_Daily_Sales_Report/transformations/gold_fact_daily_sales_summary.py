import dlt
from pyspark.sql.functions import first, sum, countDistinct

@dlt.table(
    name="daily_sales_ct_dlt.gold.fact_daily_sales_summary",
    comment="Materialized view for aggregated sales data"
)
def daily_sales_report_gold():
    sales_df = spark.read.table("LIVE.silver_mv.dim_orders").drop(
        "run_date_time", "surrogate_key"
    )
    customers_df = spark.read.table("LIVE.silver_mv.dim_customers").drop(
        "run_date_time", "surrogate_key"
    )
    transactions_df = spark.read.table("LIVE.silver_mv.dim_transactions").drop(
        "run_date_time", "surrogate_key"
    )

    # Join sales, customers and transactions data to create the daily sales summary
    join_df = sales_df.join(
        customers_df,
        sales_df.order_id == customers_df.customer_id,
        "inner"
    ).join(
        transactions_df,
        customers_df.customer_id == transactions_df.transaction_id,
        "inner"
    )

    aggr_df = (
        join_df.groupBy(
            "customer_id", "customer_name", "age", "gender", "product", "education",
            "category", "payment_method","country"
        )
        .agg(
            first("transaction_date").alias("sale_date"),
            sum("revenue").alias("total_sales_amount"),
            sum("quantity").alias("total_units_sold"),
            countDistinct("order_id").alias("unique_orders"),
            countDistinct("customer_id").alias("unique_customers")
        )
        .orderBy("customer_name", ascending=True)
    )
    
    return aggr_df