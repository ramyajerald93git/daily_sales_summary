CREATE OR REFRESH STREAMING TABLE daily_sales_ct_dlt.bronze.dim_orders_dlt
  COMMENT "Raw orders data ingested from ADLS Gen2 containers (landing)" AS
SELECT
  *
FROM
  STREAM read_files(
    "/Volumes/daily_sales_ct/bronze/raw_data/orders/",
    format => "csv",
    schemaLocation => "/Volumes/daily_sales_ct_dlt/bronze/bronze_dlt/schema/orders/",
    rescuedDataColumn => "_rescued_data"
  );

CREATE OR REFRESH STREAMING TABLE daily_sales_ct_dlt.bronze.dim_customers_dlt
  COMMENT "Raw customers data ingested from ADLS Gen2 containers (landing)" AS
SELECT
  *
FROM
  STREAM read_files(
    "/Volumes/daily_sales_ct/bronze/raw_data/customers/",
    format => "csv",
    schemaLocation => "/Volumes/daily_sales_ct_dlt/bronze/bronze_dlt/schema/customers/",
    rescuedDataColumn => "_rescued_data"
  );

CREATE OR REFRESH STREAMING TABLE daily_sales_ct_dlt.bronze.dim_transactions_dlt
  COMMENT "Raw transactions data ingested from ADLS Gen2 containers (landing)" AS
SELECT
  *
FROM
  STREAM read_files(
    "/Volumes/daily_sales_ct/bronze/raw_data/transactions/",
    format => "csv",
    schemaLocation => "/Volumes/daily_sales_ct_dlt/bronze/bronze_dlt/schema/transactions/",
    rescuedDataColumn => "_rescued_data"
  )