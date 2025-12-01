import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, when, to_date
from pyspark.sql.functions import row_number, to_date, col, when
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


args = getResolvedOptions(sys.argv, ["S3_INPUT_PATHS"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session        # standard Glue boilerplate to get spark session and glue context 
job = Job(glueContext)
job.init("create_silver_fact_data", args)    # initializes the glue job

#s3_input_paths = args["S3_INPUT_PATHS"].split(",")

s3_input_paths = json.loads(args['S3_INPUT_PATHS'])

print(f"Processing files: {s3_input_paths}")

s3_map = {path.split('/')[4]: path for path in s3_input_paths}


sfOptions = {
    "sfURL": "ZXUPR-A0887.snowflakecomputing.com",
    "sfDatabase": "SUPPLYCHAIN_ANALYTICS",
    "sfSchema": "SCM_SILVER",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",  # Optional
    "sfUser": "user_name",
    "sfPassword": "Dcd2j8Ja9ne26"
}


# Sales order data processing


df_customer = spark.read.option("header", "true") .option("inferSchema", "true").csv(s3_map.get('order_data'))
df_customer.show()





order_schema = '''
salesordernumber string,
salesorderlinenumber int,
createddate timestamp ,
processeddate timestamp ,
ordertype string,
quantity int,
unitprice double,
taxamount double,
customer_id int,
product_id int ,
supplier_id string ,
warehouse_id string ,
_corrupt_record string
'''

# Read all CSV files into a single DataFrame
#order_df = spark.read.option("header", "true").csv("s3://mar2025-training-bucket/scm/order_data/2025/03/21/orders_20250321.csv",inferSchema=True)
#order_df = spark.read.option("header", "true").csv("s3://mar2025-training-bucket/scm/order_data/2025/03/16/orders_20250316.csv",inferSchema=True)



df = spark.read.schema(order_schema).format("csv").options(header = True, mode = "PERMISSIVE", columnNameOfCorruptRecord = "_corrupt_record").load(s3_map.get('order_data'))

df=df.filter(F.col("_corrupt_record").isNull())
df.createOrReplaceTempView("salesorder")


query = f"""
SELECT 
    salesordernumber,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(taxamount AS DECIMAL(7,4)) AS taxamount,
    CAST(unitprice AS DECIMAL(8,4)) AS unitprice,
    CAST(customer_id AS INTEGER) AS customer_id,
    CAST(product_id AS INTEGER) AS product_id,
    createddate,
    processeddate,
    supplier_id,
    warehouse_id,
    
    -- Data Quality Checks
    CASE
        WHEN quantity IS NULL OR quantity < 0 THEN 'Invalid quantity'
        WHEN taxamount IS NULL OR taxamount < 0 THEN 'Invalid tax amount'
        WHEN unitprice IS NULL OR unitprice < 0 THEN 'Invalid unit price'
        WHEN customer_id IS NULL THEN 'Missing customer ID'
        WHEN product_id IS NULL THEN 'Missing product ID'
        WHEN supplier_id IS NULL THEN 'Missing supplier_id'
        WHEN warehouse_id IS NULL THEN 'Missing warehouse_id'
        WHEN createddate IS NULL OR processeddate IS NULL THEN 'Missing date'
        WHEN processeddate < createddate THEN 'Invalid date order'
        ELSE 'Valid'
    END AS error_description,
    
    CASE
        WHEN quantity IS NULL OR quantity < 0 THEN 1
        WHEN taxamount IS NULL OR taxamount < 0 THEN 1
        WHEN unitprice IS NULL OR unitprice < 0 THEN 1
        WHEN customer_id IS NULL THEN 1
        WHEN product_id IS NULL THEN 1
        WHEN supplier_id IS NULL THEN 1
        WHEN warehouse_id IS NULL THEN 1
        WHEN createddate IS NULL OR processeddate IS NULL THEN 1
        WHEN processeddate < createddate THEN 1
        ELSE 0
    END AS error_flag

FROM salesorder
"""
df_with_error_flags=spark.sql(query)
df_with_error_flags.show()
# Filter out records with error_flag as None (valid records)
valid_records_df = df_with_error_flags.filter(F.col("error_flag") ==0)
print(valid_records_df.count())
error_records_df = df_with_error_flags.filter(F.col("error_flag") == 1)
print(error_records_df.count())



# Define a window partitioned by keys to find duplicates
window_spec = Window.partitionBy("createddate", "customer_id", "product_id").orderBy("processeddate")

# Assign a row number and keep only the latest record
deduplicated_source_df = valid_records_df.withColumn("row_num", row_number().over(window_spec)).filter("row_num = 1").drop("row_num")

# Register temp views for SQL merge
deduplicated_source_df.createOrReplaceTempView("source")
deduplicated_source_df.count()


merge_query = """
MERGE INTO glue_catalog.scm_silver.fact_sales_order AS target
USING source AS source
ON target.createddate = source.createddate
AND target.customer_id = source.customer_id
AND target.product_id = source.product_id
WHEN MATCHED THEN 
    UPDATE SET *
WHEN NOT MATCHED THEN 
    INSERT * 
"""

spark.sql(merge_query).show()


# Write source temp table to a staging table in Snowflake
deduplicated_source_df.write.format("snowflake").options(**sfOptions).option("dbtable", "FACT_SALES_ORDER_STAGE").mode("overwrite").save()

# Run MERGE SQL in Snowflake
merge_sales_sql = """
MERGE INTO SUPPLYCHAIN_ANALYTICS.SCM_SILVER.FACT_SALES_ORDER AS target
USING SUPPLYCHAIN_ANALYTICS.SCM_SILVER.FACT_SALES_ORDER_STAGE AS source
ON target.CREATEDDATE = source.CREATEDDATE
AND target.CUSTOMER_ID = source.CUSTOMER_ID
AND target.PRODUCT_ID = source.PRODUCT_ID
WHEN MATCHED THEN UPDATE SET
    SALESORDERNUMBER = source.SALESORDERNUMBER,
    CREATEDDATE = source.CREATEDDATE,
    PROCESSEDDATE = source.PROCESSEDDATE,
    QUANTITY = source.QUANTITY,
    UNITPRICE = source.UNITPRICE,
    TAXAMOUNT = source.TAXAMOUNT,
    CUSTOMER_ID = source.CUSTOMER_ID,
    PRODUCT_ID = source.PRODUCT_ID,
    SUPPLIER_ID = source.SUPPLIER_ID,
    WAREHOUSE_ID = source.WAREHOUSE_ID
WHEN NOT MATCHED THEN INSERT (
    SALESORDERNUMBER,
    CREATEDDATE,
    PROCESSEDDATE,
    QUANTITY,
    UNITPRICE,
    TAXAMOUNT,
    CUSTOMER_ID,
    PRODUCT_ID,
    SUPPLIER_ID,
    WAREHOUSE_ID
) VALUES (
    source.SALESORDERNUMBER,
    source.CREATEDDATE,
    source.PROCESSEDDATE,
    source.QUANTITY,
    source.UNITPRICE,
    source.TAXAMOUNT,
    source.CUSTOMER_ID,
    source.PRODUCT_ID,
    source.SUPPLIER_ID,
    source.WAREHOUSE_ID
);

"""
spark._sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, merge_sales_sql)




# inventory fact table processing

inventory_df = spark.read.option("multiLine", True).json(s3_map.get('inventory_data'))
df_casted = inventory_df.withColumn("last_updated", to_timestamp(col("last_updated")))


window_spec = Window.partitionBy("product_id", "warehouse_id").orderBy(col("last_updated").desc())
deduped_df = df_casted.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")


deduped_df.createOrReplaceTempView("source")
deduped_df.show()

merge_query = """
MERGE INTO glue_catalog.scm_silver.fact_inventory AS target 
USING source AS source
ON target.product_id = source.product_id AND target.warehouse_id = source.warehouse_id

WHEN MATCHED THEN 
    UPDATE SET *
WHEN NOT MATCHED THEN 
    INSERT *
"""

spark.sql(merge_query)


deduped_df.write.format("snowflake").options(**sfOptions).option("dbtable", "FACT_INVENTORY_STAGE").mode("overwrite").save()

merge_inventory_sql = """
MERGE INTO SUPPLYCHAIN_ANALYTICS.SCM_SILVER.FACT_INVENTORY AS target
USING SUPPLYCHAIN_ANALYTICS.SCM_SILVER.FACT_INVENTORY_STAGE AS source
ON target.PRODUCT_ID = source.PRODUCT_ID
AND target.WAREHOUSE_ID = source.WAREHOUSE_ID
WHEN MATCHED THEN UPDATE SET
    INVENTORY_ID = source.INVENTORY_ID,
    PRODUCT_ID = source.PRODUCT_ID,
    WAREHOUSE_ID = source.WAREHOUSE_ID,
    STOCK_LEVEL = source.STOCK_LEVEL,
    LAST_UPDATED = source.LAST_UPDATED,
    DAILY_USAGE_RATE = source.DAILY_USAGE_RATE,
    INVENTORY_STATUS = source.INVENTORY_STATUS,
    LAST_MOVEMENT_DATE = source.LAST_MOVEMENT_DATE,
    PRODUCT_CATEGORY = source.PRODUCT_CATEGORY,
    REORDER_POINT = source.REORDER_POINT,
    SKU_NAME = source.SKU_NAME,
    SUPPLIER_ID = source.SUPPLIER_ID,
    UNIT_COST = source.UNIT_COST
WHEN NOT MATCHED THEN INSERT (
    INVENTORY_ID,
    PRODUCT_ID,
    WAREHOUSE_ID,
    STOCK_LEVEL,
    LAST_UPDATED,
    DAILY_USAGE_RATE,
    INVENTORY_STATUS,
    LAST_MOVEMENT_DATE,
    PRODUCT_CATEGORY,
    REORDER_POINT,
    SKU_NAME,
    SUPPLIER_ID,
    UNIT_COST
) VALUES (
    source.INVENTORY_ID,
    source.PRODUCT_ID,
    source.WAREHOUSE_ID,
    source.STOCK_LEVEL,
    source.LAST_UPDATED,
    source.DAILY_USAGE_RATE,
    source.INVENTORY_STATUS,
    source.LAST_MOVEMENT_DATE,
    source.PRODUCT_CATEGORY,
    source.REORDER_POINT,
    source.SKU_NAME,
    source.SUPPLIER_ID,
    source.UNIT_COST
);

"""
spark._sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, merge_inventory_sql)




# Shipment fact table processing

shipment_df = spark.read.option("multiLine", True).json(s3_map.get('shipment_data'))


df_casted = shipment_df \
    .withColumn("shipment_date", to_date(col("shipment_date"))) \
    .withColumn("expected_delivery_date", to_date(col("expected_delivery_date"))) \
    .withColumn("actual_delivery_date", to_date(col("actual_delivery_date"))) \
    .withColumn(
        "delivery_status",
        when(col("actual_delivery_date") > col("expected_delivery_date"), "Delayed").otherwise("On Time")
    )
    
    # Deduplicate using latest shipment_date (or actual_delivery_date if more appropriate)
window_spec = Window.partitionBy("salesordernumber").orderBy(col("shipment_date").desc())
deduped_df = df_casted.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")

deduped_df.createOrReplaceTempView("shipment_source")
deduped_df.show(2)




merge_query = """
MERGE INTO glue_catalog.scm_silver.fact_shipments AS target
USING shipment_source AS source
ON target.salesordernumber = source.salesordernumber

WHEN MATCHED THEN 
    UPDATE SET *

WHEN NOT MATCHED THEN 
    INSERT *
"""

spark.sql(merge_query)


deduped_df.write.format("snowflake").options(**sfOptions).option("dbtable", "FACT_SHIPMENTS_STAGE").mode("overwrite").save()

merge_shipments_sql = """
MERGE INTO SUPPLYCHAIN_ANALYTICS.SCM_SILVER.FACT_SHIPMENTS AS target
USING SUPPLYCHAIN_ANALYTICS.SCM_SILVER.FACT_SHIPMENTS_STAGE AS source
ON target.SALESORDERNUMBER = source.SALESORDERNUMBER
WHEN MATCHED THEN UPDATE SET
    SHIPMENT_ID = source.SHIPMENT_ID,
    SALESORDERNUMBER = source.SALESORDERNUMBER,
    SHIPMENT_DATE = source.SHIPMENT_DATE,
    DELIVERY_STATUS = source.DELIVERY_STATUS,
    EXPECTED_DELIVERY_DATE = source.EXPECTED_DELIVERY_DATE,
    ACTUAL_DELIVERY_DATE = source.ACTUAL_DELIVERY_DATE,
    CARRIER_NAME = source.CARRIER_NAME,
    DELIVERY_LOCATION = source.DELIVERY_LOCATION,
    PRODUCT_ID = source.PRODUCT_ID,
    PROMISED_DELIVERY_DATE = source.PROMISED_DELIVERY_DATE,
    PURCHASE_ORDER_ID = source.PURCHASE_ORDER_ID,
    QUANTITY_SHIPPED = source.QUANTITY_SHIPPED,
    SHIPPING_COST = source.SHIPPING_COST,
    SUPPLIER_ID = source.SUPPLIER_ID
WHEN NOT MATCHED THEN INSERT (
    SHIPMENT_ID,
    SALESORDERNUMBER,
    SHIPMENT_DATE,
    DELIVERY_STATUS,
    EXPECTED_DELIVERY_DATE,
    ACTUAL_DELIVERY_DATE,
    CARRIER_NAME,
    DELIVERY_LOCATION,
    PRODUCT_ID,
    PROMISED_DELIVERY_DATE,
    PURCHASE_ORDER_ID,
    QUANTITY_SHIPPED,
    SHIPPING_COST,
    SUPPLIER_ID
) VALUES (
    source.SHIPMENT_ID,
    source.SALESORDERNUMBER,
    source.SHIPMENT_DATE,
    source.DELIVERY_STATUS,
    source.EXPECTED_DELIVERY_DATE,
    source.ACTUAL_DELIVERY_DATE,
    source.CARRIER_NAME,
    source.DELIVERY_LOCATION,
    source.PRODUCT_ID,
    source.PROMISED_DELIVERY_DATE,
    source.PURCHASE_ORDER_ID,
    source.QUANTITY_SHIPPED,
    source.SHIPPING_COST,
    source.SUPPLIER_ID
);

"""
spark._sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, merge_shipments_sql)



# write all dim tables from iceberg  to snowflake 

dim_customer = spark.read.format("iceberg").load(f"glue_catalog.scm_silver.dim_customer")
dim_customer.write.format("snowflake").options(**sfOptions).option("dbtable","dim_customer").mode("overwrite").save()



dim_product = spark.read.format("iceberg").load(f"glue_catalog.scm_silver.dim_product")
dim_product.write.format("snowflake").options(**sfOptions).option("dbtable","dim_product").mode("overwrite").save()


dim_supplier = spark.read.format("iceberg").load(f"glue_catalog.scm_silver.dim_supplier")
dim_supplier.write.format("snowflake").options(**sfOptions).option("dbtable","dim_supplier").mode("overwrite").save()

dim_warehouse = spark.read.format("iceberg").load(f"glue_catalog.scm_silver.dim_warehouse")
dim_warehouse.write.format("snowflake").options(**sfOptions).option("dbtable","dim_warehouse").mode("overwrite").save()




job.commit()