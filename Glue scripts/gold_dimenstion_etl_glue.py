import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import row_number,col
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sfOptions = {
    "sfURL": "FYMQP-G75330.snowflakecomputing.com",
    "sfDatabase": "SUPPLYCHAIN_ANALYTICS",
    "sfSchema": "SCM_GOLD",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",  # Optional
    "sfUser": "user_name_here",
    "sfPassword": "xxxxx"
}
sc = SparkContext()               # Classic standard Glue boilerplate to get spark context
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


query = f""" SELECT 
    c.customer_name,
    p.product_name,
    s.supplier_name,
    w.warehouse_name,
    DATE(f.createddate) AS sale_date,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.quantity * f.unitprice) AS total_sales_amount,
    SUM(f.taxamount) AS total_tax_collected
FROM glue_catalog.scm_silver.fact_sales_order AS f
LEFT JOIN glue_catalog.scm_silver.dim_customer AS c
    ON f.customer_id = c.customer_id
LEFT JOIN glue_catalog.scm_silver.dim_product AS p
    ON f.product_id = p.product_id
LEFT JOIN glue_catalog.scm_silver.dim_supplier AS s
    ON f.supplier_id = s.supplier_id
LEFT JOIN glue_catalog.scm_silver.dim_warehouse AS w
    ON f.warehouse_id = w.warehouse_id
GROUP BY 
    c.customer_name, 
    p.product_name, 
    s.supplier_name, 
    w.warehouse_name, 
    DATE(f.createddate)
ORDER BY total_sales_amount DESC;

"""
df_sales_summary=spark.sql(query)
df_sales_summary.createOrReplaceTempView("sales_summary_updates")
df_sales_summary.show()
print(f"Starting merge fact_sales_summary ")


# Perform MERGE INTO using Iceberg SQL

# 1. Sales Summary

spark.sql("""
    MERGE INTO glue_catalog.scm_gold.fact_sales_summary target
    USING sales_summary_updates source
    ON target.customer_name = source.customer_name
        AND target.product_name = source.product_name
        AND target.supplier_name = source.supplier_name
        AND target.warehouse_name = source.warehouse_name
        AND target.sale_date = source.sale_date
    WHEN MATCHED THEN
        UPDATE SET
            target.total_quantity_sold = source.total_quantity_sold,
            target.total_sales_amount = source.total_sales_amount,
            target.total_tax_collected = source.total_tax_collected
    WHEN NOT MATCHED THEN
        INSERT *
""")

# ------------------------------------------------------------------------------------------------------------------------------------------------



# 2. Sales Summary By Country
print(f"Starting merge fact_sales_summary_by_country ")


spark.sql(f"""
MERGE INTO glue_catalog.scm_gold.fact_sales_summary_by_country AS tgt
USING (
    SELECT 
        c.country,
        SUM(f.quantity * f.unitprice) AS total_sales_amount,
        SUM(f.quantity) AS total_quantity_sold,
        COUNT(DISTINCT f.salesordernumber) AS total_orders,
        CURRENT_TIMESTAMP() AS last_updated
    FROM glue_catalog.scm_silver.fact_sales_order AS f
    LEFT JOIN glue_catalog.scm_silver.dim_customer AS c
        ON f.customer_id = c.customer_id
    GROUP BY c.country
) AS src
ON tgt.country = src.country
WHEN MATCHED THEN 
    UPDATE SET 
        tgt.total_sales_amount = src.total_sales_amount,
        tgt.total_quantity_sold = src.total_quantity_sold,
        tgt.total_orders = src.total_orders,
        tgt.last_updated = src.last_updated
WHEN NOT MATCHED THEN 
    INSERT (country, total_sales_amount, total_quantity_sold, total_orders, last_updated)
    VALUES (src.country, src.total_sales_amount, src.total_quantity_sold, src.total_orders, src.last_updated);

""")

# ------------------------------------------------------------------------------------------------------------------------------------------------



# 3. Sales Summary By Supplier Daily 
print(f"Starting merge fact_sales_summary_by_supplier_daily ")

spark.sql(f"""
MERGE INTO glue_catalog.scm_gold.fact_sales_summary_by_supplier_daily AS tgt
USING (
    SELECT 
        DATE(f.createddate) AS sales_date,
        s.supplier_id,
        s.supplier_name,
        SUM(f.quantity * f.unitprice) AS total_sales_amount,
        SUM(f.quantity) AS total_quantity_sold,
        COUNT(DISTINCT f.salesordernumber) AS total_orders,
        CURRENT_TIMESTAMP() AS last_updated
    FROM glue_catalog.scm_silver.fact_sales_order AS f
    LEFT JOIN glue_catalog.scm_silver.dim_supplier AS s
        ON f.supplier_id = s.supplier_id
    GROUP BY DATE(f.createddate), s.supplier_id, s.supplier_name
) AS src
ON tgt.sales_date = src.sales_date AND tgt.supplier_id = src.supplier_id
WHEN MATCHED THEN 
    UPDATE SET 
        tgt.total_sales_amount = src.total_sales_amount,
        tgt.total_quantity_sold = src.total_quantity_sold,
        tgt.total_orders = src.total_orders,
        tgt.last_updated = src.last_updated
WHEN NOT MATCHED THEN 
    INSERT (sales_date, supplier_id, supplier_name, total_sales_amount, total_quantity_sold, total_orders, last_updated)
    VALUES (src.sales_date, src.supplier_id, src.supplier_name, src.total_sales_amount, src.total_quantity_sold, src.total_orders, src.last_updated);


""")

# ------------------------------------------------------------------------------------------------------------------------------------------------



# 4. Sales Summary By Supplier Monthly 
print(f"Starting merge fact_sales_summary_by_supplier_monthly ")

spark.sql(f"""
MERGE INTO glue_catalog.scm_gold.fact_sales_summary_by_supplier_monthly AS tgt
USING (
    SELECT 
        DATE_FORMAT(f.createddate, 'yyyy-MM') AS sales_month,
        s.supplier_id,
        s.supplier_name,
        SUM(f.quantity * f.unitprice) AS total_sales_amount,
        SUM(f.quantity) AS total_quantity_sold,
        COUNT(DISTINCT f.salesordernumber) AS total_orders,
        CURRENT_TIMESTAMP() AS last_updated
    FROM glue_catalog.scm_silver.fact_sales_order AS f
    LEFT JOIN glue_catalog.scm_silver.dim_supplier AS s
        ON f.supplier_id = s.supplier_id
    GROUP BY DATE_FORMAT(f.createddate, 'yyyy-MM'), s.supplier_id, s.supplier_name
) AS src
ON tgt.sales_month = src.sales_month AND tgt.supplier_id = src.supplier_id
WHEN MATCHED THEN 
    UPDATE SET 
        tgt.total_sales_amount = src.total_sales_amount,
        tgt.total_quantity_sold = src.total_quantity_sold,
        tgt.total_orders = src.total_orders,
        tgt.last_updated = src.last_updated
WHEN NOT MATCHED THEN 
    INSERT (sales_month, supplier_id, supplier_name, total_sales_amount, total_quantity_sold, total_orders, last_updated)
    VALUES (src.sales_month, src.supplier_id, src.supplier_name, src.total_sales_amount, src.total_quantity_sold, src.total_orders, src.last_updated);


""")

merge_view_query = """
SELECT
    f.salesordernumber,
    f.createddate,
    f.processeddate,
    f.quantity,
    f.unitprice,
    f.taxamount,
    f.customer_id,
    c.customer_name,
    f.product_id,
    p.product_name,
    p.supplier_id,
    s.supplier_name,
    w.warehouse_id,
    w.warehouse_name,
    i.stock_level AS current_stock_level,
    sh.shipment_date,
    sh.delivery_status
FROM glue_catalog.scm_silver.fact_sales_order f
JOIN glue_catalog.scm_silver.dim_customer c ON f.customer_id = c.customer_id
JOIN glue_catalog.scm_silver.dim_product p ON f.product_id = p.product_id
JOIN glue_catalog.scm_silver.dim_supplier s ON f.supplier_id = s.supplier_id
JOIN glue_catalog.scm_silver.dim_warehouse w ON f.warehouse_id = w.warehouse_id
LEFT JOIN glue_catalog.scm_silver.fact_inventory i 
    ON f.product_id = i.product_id 
LEFT JOIN glue_catalog.scm_silver.fact_shipments sh 
    ON f.salesordernumber = sh.salesordernumber
"""




fact_supply_chain_summary=spark.sql(merge_view_query)

# Deduplicate using shipment_date or createddate
window_spec = Window.partitionBy("salesordernumber").orderBy(col("shipment_date").desc_nulls_last())
deduped_df = fact_supply_chain_summary.withColumn("row_num", row_number().over(window_spec)).filter("row_num = 1").drop("row_num")

deduped_df.createOrReplaceTempView("source_summary")

deduped_df.show()



# ------------------------------------------------------------------------------------------------------------------------------------------------



# 5. Supply Chain Summary

print(f"Starting merge fact_supply_chain_summary ")

# Perform MERGE INTO using Iceberg SQL


spark.sql("""
MERGE INTO glue_catalog.scm_gold.fact_supply_chain_summary AS target
USING source_summary AS source
ON target.salesordernumber = source.salesordernumber

WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

""")

# ------------------------------------------------------------------------------------------------------------------------------------------------



# 6. Shipping Performance

print(f"Starting merge fact_shipping_performance ")

# Measure shipping efficiency like delays, delivery duration, etc.



spark.sql("""
CREATE or REPLACE TABLE glue_catalog.scm_gold.fact_shipping_performance AS
SELECT 
    f.salesordernumber,
    s.shipment_date,
    s.expected_delivery_date,
    s.actual_delivery_date,
    DATEDIFF(s.actual_delivery_date, s.shipment_date) AS delivery_duration_days,
    CASE 
        WHEN s.actual_delivery_date > s.expected_delivery_date THEN 'Delayed'
        ELSE 'On Time'
    END AS delivery_status
FROM glue_catalog.scm_silver.fact_sales_order f
JOIN glue_catalog.scm_silver.fact_shipments s ON f.salesordernumber = s.salesordernumber;

""")


# 7. Aggregated Inventory Levels 

print(f"Starting merge aggregated_inventory_levels ")


# Show stock availability trends (e.g., daily or weekly).
spark.sql("""
CREATE or REPLACE TABLE glue_catalog.scm_gold.aggregated_inventory_levels AS
SELECT 
    i.last_updated,
    i.product_id,
    p.product_name,
    i.warehouse_id,
    w.warehouse_name,
    SUM(i.stock_level) AS total_stock
FROM glue_catalog.scm_silver.fact_inventory i
JOIN glue_catalog.scm_silver.dim_product p ON i.product_id = p.product_id
JOIN glue_catalog.scm_silver.dim_warehouse w ON i.warehouse_id = w.warehouse_id
GROUP BY i.last_updated, i.product_id, p.product_name, i.warehouse_id, w.warehouse_name;
""")

# ------------------------------------------------------------------------------------------------------------------------------------------------




# 8. late_deliveries_daily  

print(f"Starting  late_deliveries_daily ")

# Today's date
today = date.today()

# Run the query
daily_df = spark.sql("""
SELECT
  fs.salesordernumber,
  fs.createddate,
  fship.expected_delivery_date,
  fship.actual_delivery_date,
  fship.delivery_status
FROM glue_catalog.scm_silver.fact_sales_order fs
LEFT JOIN glue_catalog.scm_silver.fact_shipments fship
  ON fs.salesordernumber = fship.salesordernumber
WHERE fship.expected_delivery_date < CURRENT_DATE
  AND (fship.actual_delivery_date IS NULL OR fship.actual_delivery_date > fship.expected_delivery_date)
""").withColumn("run_date", lit(today))

# Append the data
daily_df.writeTo("glue_catalog.scm_gold.late_deliveries_daily").append()
daily_df.write.format("snowflake").options(**sfOptions).option("dbtable", "late_deliveries_daily").mode("overwrite").save()

# ------------------------------------------------------------------------------------------------------------------------------------------------



# 9. inventory_overstock_inactive

print(f"Starting  inventory_overstock_inactive ")

# Run the query
overstock_df = spark.sql("""
SELECT
  inventory_id,
  product_id,
  stock_level,
  reorder_point,
  TO_DATE(last_movement_date) AS last_movement_date,
  DATEDIFF(CURRENT_DATE, TO_DATE(last_movement_date)) AS days_since_last_movement
FROM glue_catalog.scm_silver.fact_inventory
WHERE stock_level > reorder_point * 2
  AND DATEDIFF(CURRENT_DATE, TO_DATE(last_movement_date)) > 30
""").withColumn("run_date", lit(today))    #  This is adding today's ḍate


# Optional cleanup: Before appending, removing the today's data to avoid duplicate records

spark.sql(f"""
DELETE FROM glue_catalog.scm_gold.inventory_overstock_inactive
WHERE run_date = DATE('{today}')
""")

# Append today's data
overstock_df.writeTo("glue_catalog.scm_gold.inventory_overstock_inactive").append()
overstock_df.write.format("snowflake").options(**sfOptions).option("dbtable", "inventory_overstock_inactive").mode("overwrite").save()


# ------------------------------------------------------------------------------------------------------------------------------------------------





# 10. high_shipping_orders 
print(f"Starting  high_shipping_orders ")

# Query with aggregation and filters
shipping_df = spark.sql("""
SELECT
  salesordernumber,
  SUM(shipping_cost) AS total_shipping_cost,
  COUNT(DISTINCT shipment_id) AS shipments_per_order
FROM glue_catalog.scm_silver.fact_shipments
GROUP BY salesordernumber
HAVING total_shipping_cost > 800 OR shipments_per_order > 3
""").withColumn("run_date", lit(today))

# Same as above, removing today's data if already exists to avoid duplicates

spark.sql(f"""
DELETE FROM glue_catalog.scm_gold.high_shipping_orders
WHERE run_date = DATE('{today}')
""")

# Append to gold table
shipping_df.writeTo("glue_catalog.scm_gold.high_shipping_orders").append()
shipping_df.write.format("snowflake").options(**sfOptions).option("dbtable", "high_shipping_orders").mode("overwrite").save()

# ------------------------------------------------------------------------------------------------------------------------------------------------




# 11. order_stock_shortage
 
print(f"Starting  order_stock_shortage ")


# Query + run_date column
shortage_df = spark.sql("""
SELECT
  fs.salesordernumber,
  fs.createddate,
  fs.processeddate,
  DATEDIFF(fs.processeddate, fs.createddate) AS order_processing_delay,
  fi.stock_level,
  fi.warehouse_id
FROM glue_catalog.scm_silver.fact_sales_order fs
JOIN glue_catalog.scm_silver.fact_inventory fi
  ON fs.product_id = fi.product_id AND fs.warehouse_id = fi.warehouse_id
WHERE fi.stock_level < fs.quantity
""").withColumn("run_date", lit(today))

# Optional: remove duplicates for today before inserting
spark.sql(f"""
DELETE FROM glue_catalog.scm_gold.order_stock_shortage
WHERE run_date = DATE('{today}')
""")

# Append today's data
shortage_df.writeTo("glue_catalog.scm_gold.order_stock_shortage").append()
shortage_df.write.format("snowflake").options(**sfOptions).option("dbtable", "order_stock_shortage").mode("overwrite").save()

# ------------------------------------------------------------------------------------------------------------------------------------------------



# 12. supplier_delivery_performance

print(f"Starting  supplier_delivery_performance ")


# Run the query
performance_df = spark.sql("""
SELECT
  supplier_id,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN actual_delivery_date > expected_delivery_date THEN 1 ELSE 0 END) AS delayed_shipments,
  ROUND(100.0 * SUM(CASE WHEN actual_delivery_date <= expected_delivery_date THEN 1 ELSE 0 END) / COUNT(*), 2) AS on_time_pct
FROM glue_catalog.scm_silver.fact_shipments
GROUP BY supplier_id
""").withColumn("run_date", lit(today))

# Optional: delete today's data to prevent duplicate inserts
spark.sql(f"""
DELETE FROM glue_catalog.scm_gold.supplier_delivery_performance
WHERE run_date = DATE('{today}')
""")

# Append today's result
performance_df.writeTo("glue_catalog.scm_gold.supplier_delivery_performance").append()

performance_df.write.format("snowflake").options(**sfOptions).option("dbtable", "supplier_delivery_performance").mode("overwrite").save()

# ------------------------------------------------------------------------------------------------------------------------------------------------


# 13.upcoming_stock_shortage

print(f"Starting  upcoming_stock_shortage ")


# Run the query
shortage_df = spark.sql("""
WITH upcoming_demand AS (
  SELECT product_id, SUM(quantity) AS total_demand
  FROM glue_catalog.scm_silver.fact_sales_order
  WHERE createddate BETWEEN CURRENT_DATE AND DATE_ADD(CURRENT_DATE, 14)
  GROUP BY product_id
)
SELECT
  i.product_id,
  i.stock_level,
  u.total_demand,
  i.stock_level - u.total_demand AS net_stock
FROM glue_catalog.scm_silver.fact_inventory i
JOIN upcoming_demand u ON i.product_id = u.product_id
WHERE i.stock_level - u.total_demand < 0
""").withColumn("run_date", lit(today))

# Optional: delete today's data if already exists
spark.sql(f"""
DELETE FROM glue_catalog.scm_gold.upcoming_stock_shortage
WHERE run_date = DATE('{today}')
""")

# Append today's results
shortage_df.writeTo("glue_catalog.scm_gold.upcoming_stock_shortage").append()
shortage_df.write.format("snowflake").options(**sfOptions).option("dbtable", "upcoming_stock_shortage").mode("overwrite").save()

# ------------------------------------------------------------------------------------------------------------------------------------------------


# 14. sales_order_lead_time

print(f"Starting  sales_order_lead_time ")


# Execute query
lead_time_df = spark.sql("""
SELECT
  fs.salesordernumber,
  fs.createddate,
  MAX(fship.actual_delivery_date) AS last_delivery,
  DATEDIFF(MAX(fship.actual_delivery_date), fs.createddate) AS total_lead_time
FROM glue_catalog.scm_silver.fact_sales_order fs
JOIN glue_catalog.scm_silver.fact_shipments fship 
  ON fs.salesordernumber = fship.salesordernumber
GROUP BY fs.salesordernumber, fs.createddate
""").withColumn("run_date", lit(today))

# Optional: delete today’s rows if needed to prevent duplicates
spark.sql(f"""
DELETE FROM glue_catalog.scm_gold.sales_order_lead_time
WHERE run_date = DATE('{today}')
""")

# Append today's data
lead_time_df.writeTo("glue_catalog.scm_gold.sales_order_lead_time").append()
lead_time_df.write.format("snowflake").options(**sfOptions).option("dbtable", "sales_order_lead_time").mode("overwrite").save()


job.commit()