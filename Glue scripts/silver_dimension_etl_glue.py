import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# -----------------------------
# Glue Context
# -----------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session       # standard Glue boilerplate to get spark session and glue context
job = Job(glueContext)
job.init(args['JOB_NAME'], args)        # initializes the glue job

# -----------------------------
# MySQL Config  -  connection details
# -----------------------------
mysql_options = {
    "url": "jdbc:mysql://104.237.2.219:5340/scm_db",
    "user": "mysqluser",
    "password": "*7E567C9DC06217268D72D52BABCA14EAB8993ACF",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# -----------------------------
# Table Configs for Iceberg Lakehouse
# -----------------------------
table_configs = [
    {
        "name": "Customer",
        "pk": "CustomerID",
        "ts_col": "UpdatedAt",                                # timestamp column for incremental load for glue bookmark
        "iceberg_table": "glue_catalog.scm_bronze.customer"   # Iceberg target table
    },
    {
        "name": "Supplier",
        "pk": "SupplierID",
        "ts_col": "UpdatedAt",
        "iceberg_table": "glue_catalog.scm_bronze.supplier"
    },
    {
        "name": "Warehouse",
        "pk": "WarehouseID",
        "ts_col": "UpdatedAt",
        "iceberg_table": "glue_catalog.scm_bronze.warehouse"
    },
    {
        "name": "Product",
        "pk": "ProductID",
        "ts_col": "UpdatedAt",
        "iceberg_table": "glue_catalog.scm_bronze.product"
    }
]

# -----------------------------
# Read with Glue Bookmark
# -----------------------------
def read_incremental_data_with_glue(glueContext, table_name, ts_col, mysql_options):
    """
    Reads incremental data from MySQL using Glue bookmarks.
    """
    datasource = glueContext.create_dynamic_frame_from_options(
        connection_type="mysql",
        connection_options={
            "url": mysql_options["url"],
            "user": mysql_options["user"],
            "password": mysql_options["password"],
            "dbtable": table_name,
            "jobBookmarkKeys": [ts_col],
            "jobBookmarkKeysSortOrder": "asc"
        },
        transformation_ctx=f"datasource_{table_name}"
    )
    return datasource.toDF()      # returns as Spark DataFrame

# -----------------------------
# Core Merge Helpers
# -----------------------------
def merge_into_iceberg(spark, incoming_df, iceberg_table, pk):
    incoming_df.createOrReplaceTempView("source")

    cols = [c for c in incoming_df.columns if c != pk]
    update_set = ",\n    ".join([f"target.{c} = source.{c}" for c in cols])
    insert_cols = ", ".join([pk] + cols)
    insert_vals = ", ".join([f"source.{c}" for c in [pk] + cols])

    merge_query = f"""
    MERGE INTO {iceberg_table} AS target
    USING source AS source
    ON target.{pk} = source.{pk}
    WHEN MATCHED THEN UPDATE SET
        {update_set}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})
    """
    spark.sql(merge_query)

# -----------------------------
# Transformations (Customer, Product, Supplier, Warehouse)
# -----------------------------
def transform_customer(spark, df, view_name="df_customer_raw"):
    df.createOrReplaceTempView(view_name)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW transformed_customers AS
        SELECT
            CAST(CustomerID AS INT) AS customer_id,
            CONCAT(FirstName, ' ', LastName) AS customer_name,
            EmailAddress AS email,
            Phone AS phone,
            City AS region,
            CountryRegion AS country,
            CompanyName AS customer_type,
            'Bronze' AS loyalty_status,
            CURRENT_TIMESTAMP AS start_date,
            CAST(NULL AS TIMESTAMP) AS end_date,
            TRUE AS is_current,
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM {view_name}
    """)
    deduped_df = spark.sql("""
        SELECT *
        FROM (
            SELECT t.*,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id ORDER BY customer_id DESC
                ) AS row_num
            FROM transformed_customers t
        ) tmp
        WHERE row_num = 1
    """)
    deduped_df.createOrReplaceTempView("new_customers_deduped")
    return deduped_df

def merge_customer_scd2(spark):
    # Step 1: expire old records
    spark.sql("""
    MERGE INTO glue_catalog.scm_silver.dim_customer AS target
    USING new_customers_deduped AS source
    ON target.customer_id = source.customer_id AND target.is_current = True
    WHEN MATCHED AND (
        target.customer_name <> source.customer_name OR
        target.email <> source.email OR
        target.phone <> source.phone OR
        target.region <> source.region OR
        target.country <> source.country OR
        target.customer_type <> source.customer_type OR
        target.loyalty_status <> source.loyalty_status
    ) THEN
      UPDATE SET
        target.is_current = False,
        target.end_date = current_timestamp(),
        target.updated_at = current_timestamp()
    """)
    # Step 2: insert new
    spark.sql("""
    MERGE INTO glue_catalog.scm_silver.dim_customer AS target
    USING new_customers_deduped AS source
    ON target.customer_id = source.customer_id AND target.is_current = True
    WHEN NOT MATCHED THEN
      INSERT (
          customer_id, customer_name, email, phone, region, country,
          customer_type, loyalty_status, start_date, end_date,
          is_current, created_at, updated_at
      )
      VALUES (
          source.customer_id, source.customer_name, source.email, source.phone, 
          source.region, source.country, source.customer_type, source.loyalty_status,
          current_timestamp(), NULL, True, current_timestamp(), current_timestamp()
      )
    """)


	
def transform_product(spark, df_product, view_name="product_src"):
    # Register the dataframe as a temp view
    df_product.createOrReplaceTempView(view_name)

    # Run SQL transformation
    query = f"""
        SELECT
            CAST(ProductID AS INT) AS product_id,
            Name AS product_name,
            ProductCategoryID AS category,
            ProductModelID AS sub_category,
            'Unknown' AS brand,              -- Default brand
            'Unknown' AS supplier_id,        -- Default supplier
            CAST(ListPrice AS DOUBLE) AS unit_price,
            CAST(StandardCost AS DOUBLE) AS cost_price,
            CAST(SellStartDate AS TIMESTAMP) AS sell_start_date,
            ThumbnailPhotoFileName AS thumbnail_photo,
            CURRENT_TIMESTAMP() AS start_date,
            CAST(NULL AS TIMESTAMP) AS end_date,
            TRUE AS is_current,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
        FROM {view_name}
    """
    transform_product_df=spark.sql(query)
    transform_product_df.createOrReplaceTempView("transform_product")


def merge_product_dim(spark):
    merge_query = """
    MERGE INTO glue_catalog.scm_silver.dim_product AS target
    USING transform_product AS source
    ON target.product_id = source.product_id
    
    -- Update if product exists
    WHEN MATCHED THEN 
      UPDATE SET
        target.product_name = source.product_name,
        target.category = source.category,
        target.sub_category = source.sub_category,
        target.brand = source.brand,
        target.supplier_id = source.supplier_id,
        target.unit_price = source.unit_price,
        target.cost_price = source.cost_price,
        target.sell_start_date = source.sell_start_date,
        target.thumbnail_photo = source.thumbnail_photo,
        target.updated_at = current_timestamp()
    
    -- Insert if product does not exist
    WHEN NOT MATCHED THEN 
      INSERT (
        product_id, product_name, category, sub_category, brand, supplier_id, 
        unit_price, cost_price, sell_start_date, thumbnail_photo,
        created_at, updated_at
      ) VALUES (
        source.product_id, source.product_name, source.category, source.sub_category, 
        source.brand, source.supplier_id, source.unit_price, source.cost_price, 
        source.sell_start_date, source.thumbnail_photo,
        current_timestamp(), current_timestamp()
      )
    """
    
    spark.sql(merge_query)



def transform_supplier(spark, df_supplier, view_name="df_supplier"):
    # Register input as a temp view
    df_supplier.createOrReplaceTempView(view_name)

    # SQL query for transformation + deduplication
    query = f"""
        WITH ranked_suppliers AS (
            SELECT
                SupplierID AS supplier_id,
                SupplierName AS supplier_name,
                ContactName AS contact_name,
                Email AS email,
                Phone AS phone,
                Address AS address,
                City AS city,
                State AS state,
                Country AS country,
                PostalCode AS postal_code,
                BusinessCategory AS business_category,
                CURRENT_TIMESTAMP() AS start_date,
                CAST(NULL AS TIMESTAMP) AS end_date,
                TRUE AS is_current,
                CreatedAt AS created_at,
                UpdatedAt AS updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY SupplierID
                    ORDER BY UpdatedAt DESC
                ) AS row_num
            FROM {view_name}
        )
        SELECT
            supplier_id,
            supplier_name,
            contact_name,
            email,
            phone,
            address,
            city,
            state,
            country,
            postal_code,
            business_category,
            start_date,
            end_date,
            is_current,
            created_at,
            updated_at
        FROM ranked_suppliers
        WHERE row_num = 1
    """

    transform_supplier_df = spark.sql(query)
    transform_supplier_df.createOrReplaceTempView("transform_supplier")
    return transform_supplier_df


def merge_supplier_dim(spark):
    # Step 1: Expire old records where supplier details changed
    expire_query = """
    MERGE INTO glue_catalog.scm_silver.dim_supplier AS target
    USING transform_supplier AS source
    ON target.supplier_id = source.supplier_id AND target.is_current = TRUE
    
    WHEN MATCHED AND (
        target.supplier_name <> source.supplier_name OR
        target.contact_name <> source.contact_name OR
        target.email <> source.email OR
        target.phone <> source.phone OR
        target.address <> source.address OR
        target.city <> source.city OR
        target.state <> source.state OR
        target.country <> source.country OR
        target.postal_code <> source.postal_code OR
        target.business_category <> source.business_category
    ) THEN 
        UPDATE SET 
            target.is_current = FALSE,
            target.end_date = current_timestamp(),
            target.updated_at = current_timestamp()
    """
    spark.sql(expire_query)

    # Step 2: Insert new versions (changed + new suppliers)
    insert_query = """
    MERGE INTO glue_catalog.scm_silver.dim_supplier AS target
    USING transform_supplier AS source
    ON target.supplier_id = source.supplier_id AND target.is_current = TRUE
    
    WHEN NOT MATCHED THEN 
        INSERT (
            supplier_id, supplier_name, contact_name, email, phone, address, 
            city, state, country, postal_code, business_category, 
            start_date, end_date, is_current, created_at, updated_at
        ) VALUES (
            source.supplier_id, source.supplier_name, source.contact_name, 
            source.email, source.phone, source.address, source.city, source.state, 
            source.country, source.postal_code, source.business_category, 
            current_timestamp(), NULL, TRUE, current_timestamp(), current_timestamp()
        )
    """
    spark.sql(insert_query)



def transform_warehouse(spark, df_warehouse, view_name="df_warehouse"):
    # Register input as temp view
    df_warehouse.createOrReplaceTempView(view_name)

    # SQL query for transformation + deduplication
    query = f"""
        WITH ranked_warehouse AS (
            SELECT
                WarehouseID AS warehouse_id,
                WarehouseName AS warehouse_name,
                Location AS location,
                City AS city,
                State AS state,
                Country AS country,
                PostalCode AS postal_code,
                CAST(Capacity AS INT) AS capacity,
                CURRENT_TIMESTAMP() AS start_date,
                CAST(NULL AS TIMESTAMP) AS end_date,
                TRUE AS is_current,
                CreatedAt AS created_at,
                UpdatedAt AS updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY WarehouseID
                    ORDER BY UpdatedAt DESC
                ) AS row_num
            FROM {view_name}
        )
        SELECT
            warehouse_id,
            warehouse_name,
            location,
            city,
            state,
            country,
            postal_code,
            capacity,
            start_date,
            end_date,
            is_current,
            created_at,
            updated_at
        FROM ranked_warehouse
        WHERE row_num = 1
    """

    transform_warehouse_df = spark.sql(query)
    transform_warehouse_df.createOrReplaceTempView("transform_warehouse")
    return transform_warehouse_df


def merge_warehouse_dim(spark):
    query = """
    MERGE INTO glue_catalog.scm_silver.dim_warehouse AS target
    USING transform_warehouse AS source
    ON target.warehouse_id = source.warehouse_id
    
    -- Update existing warehouse
    WHEN MATCHED THEN
      UPDATE SET
        target.warehouse_name = source.warehouse_name,
        target.location = source.location,
        target.city = source.city,
        target.state = source.state,
        target.country = source.country,
        target.postal_code = source.postal_code,
        target.capacity = source.capacity,
        target.updated_at = current_timestamp()
    
    -- Insert new warehouse
    WHEN NOT MATCHED THEN
      INSERT (
        warehouse_id, warehouse_name, location, city, state, country, 
        postal_code, capacity, created_at, updated_at
      ) VALUES (
        source.warehouse_id, source.warehouse_name, source.location, source.city, 
        source.state, source.country, source.postal_code, source.capacity, 
        current_timestamp(), current_timestamp()
      )
    """
    spark.sql(query)

# -----------------------------
# Process One Table
# -----------------------------
def process_table_with_glue_bookmark(glueContext, mysql_options, cfg):
    table = cfg["name"]
    pk = cfg["pk"]
    ts_col = cfg["ts_col"]
    iceberg_table = cfg["iceberg_table"]

    # Step 1: Read incremental data
    incoming_df = read_incremental_data_with_glue(glueContext, table, ts_col, mysql_options)
    count = incoming_df.count()
    print(f"🚀 Incremental data fetched for {table}: {count} rows")

    if count == 0:
        print(f"⚠️ No new changes for {table}. Skipping...")
        return

    # Step 2: Merge Bronze
    merge_into_iceberg(spark, incoming_df, iceberg_table, pk)
    print(f"✅ MERGE done for {table} Bronze catalog")

    # Step 3: Transform & Merge Silver
    table_lower = table.lower()
    if table_lower == "customer":
        transform_customer(spark, incoming_df)
        merge_customer_scd2(spark)
        print("✅ Customer SCD2 merge done in Silver")
    elif table_lower == "product":
        transform_product(spark, incoming_df)
        merge_product_dim(spark)
        print("✅ Product merge done in Silver")
    elif table_lower == "supplier":
        transform_supplier(spark, incoming_df)
        merge_supplier_dim(spark)
        print("✅ Supplier merge done in Silver")
    elif table_lower == "warehouse":
        transform_warehouse(spark, incoming_df)
        merge_warehouse_dim(spark)
        print("✅ Warehouse merge done in Silver")
    else:
        print(f"ℹ️ No special transformation for {table}")

# -----------------------------
# Run for All Tables
# -----------------------------
for cfg in table_configs:
    process_table_with_glue_bookmark(glueContext, mysql_options, cfg)

job.commit()
