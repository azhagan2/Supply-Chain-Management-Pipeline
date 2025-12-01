import json
import boto3
import zipfile
import os
import tempfile
import csv
import io
from datetime import datetime

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# GLUE_JOB_NAME = "process_sales_data"

# ====================== EXPECTED SCHEMAS ======================
EXPECTED_SCHEMAS = {
    "sales_orders": [
        'salesordernumber', 'salesorderlinenumber', 'createddate', 'processeddate',
        'ordertype', 'quantity', 'unitprice', 'taxamount', 'customer_id',
        'product_id', 'supplier_id', 'warehouse_id'
    ],
    "inventory": [
        'inventory_id', 'product_id', 'sku_name', 'warehouse_id', 'stock_level',
        'daily_usage_rate', 'unit_cost', 'reorder_point', 'product_category',
        'supplier_id', 'inventory_status', 'last_movement_date', 'last_updated'
    ],
    "shipment": [
        'shipment_id', 'salesordernumber', 'purchase_order_id', 'product_id',
        'supplier_id', 'carrier_name', 'shipment_date', 'promised_delivery_date',
        'expected_delivery_date', 'actual_delivery_date', 'quantity_shipped',
        'shipping_cost', 'delivery_status', 'delivery_location'
    ]
}


# ====================== HELPERS ======================
def upload_to_s3(file_path, bucket_name, new_key):
    s3_client.upload_file(file_path, bucket_name, new_key)
    print(f"✅ Uploaded: {new_key}")


def ensure_crawler_exists(crawler_name, s3_targets, database_name="scm_bronze"):
    try:
        glue_client.get_crawler(Name=crawler_name)
        print(f"Crawler '{crawler_name}' already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Creating crawler '{crawler_name}'...")
        glue_client.create_crawler(
            Name=crawler_name,
            Role="GlueDevRoleCA",
            DatabaseName=database_name,
            Targets={"S3Targets": [{"Path": "/".join(path.split("/")[:5]) + "/"} for path in s3_targets]},
            SchemaChangePolicy={"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "DEPRECATE_IN_DATABASE"},
        )
    print(f"Starting crawler: {crawler_name}")
    glue_client.start_crawler(Name=crawler_name)


def get_csv_sample(bucket, key, sample_size=1000):
    """Fetch sample rows and headers from CSV in S3."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    stream = io.TextIOWrapper(response['Body'], encoding='utf-8')
    reader = csv.DictReader(stream)
    headers = reader.fieldnames
    sample = [row for _, row in zip(range(sample_size), reader)]
    return headers, sample


def get_json_sample(bucket, key, sample_size=1000):
    """Fetch first N records from JSON or JSON Lines in S3."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')

    try:
        data = json.loads(content)
        if isinstance(data, dict):
            data = [data]
    except json.JSONDecodeError:
        data = [json.loads(line) for line in content.splitlines() if line.strip()]

    return data[:sample_size]


def validate_csv_schema_s3(bucket, key, expected_columns):
    """Validate CSV schema using a small sample."""
    headers, _ = get_csv_sample(bucket, key)
    if headers != expected_columns:
        raise ValueError(f"CSV schema mismatch. Expected {expected_columns}, got {headers}")
    print(f"✅ CSV schema validation passed for {key}")


def validate_json_schema_s3(bucket, key, expected_fields):
    """Validate JSON schema using a small sample."""
    sample_data = get_json_sample(bucket, key)
    if not sample_data:
        raise ValueError(f"Empty or invalid JSON data in {key}")

    record_keys = list(sample_data[0].keys())
    missing = [field for field in expected_fields if field not in record_keys]
    if missing:
        raise ValueError(f"JSON schema mismatch. Missing keys: {missing}")
    print(f"✅ JSON schema validation passed for {key}")


# def trigger_glue_etl(bucket_name, file_paths):
#     response = glue_client.start_job_run(
#         JobName=GLUE_JOB_NAME,
#         Arguments={"--S3_INPUT_PATHS": ",".join(file_paths), "--JOB_NAME": GLUE_JOB_NAME}
#     )
#     print(f"🚀 Glue job triggered: {response['JobRunId']}")



# Process starts here

# ====================== MAIN PROCESS ======================
def process_and_upload_zip(bucket_name, zip_key):
    if not zip_key.endswith(".zip"):
        print("Not a ZIP file. Skipping.")
        return

    print(f"Processing ZIP: {zip_key}")

    with tempfile.TemporaryDirectory() as tmpdirname:
        zip_path = os.path.join(tmpdirname, "data.zip")
        s3_client.download_file(bucket_name, zip_key, zip_path)

        # Extract ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmpdirname)

        process_datetime = datetime.now().strftime("%Y/%m/%d/%H")
        file_datetime = datetime.now().strftime("%Y/%m/%d/%H")

        file_mappings = {
            "sales_orders": f"scm_bronze/order_data/orders_{file_datetime}.csv",
            "inventory": f"scm_bronze/inventory_data/inventory_{file_datetime}.json",
            "shipment": f"scm_bronze/shipment_data/shipment_{file_datetime}.json",
        }

        uploaded_files = []
        error_folder = f"scm_bronze/error_data/{process_datetime}/"

        print("Processing extracted files...")

        for filename in os.listdir(tmpdirname):
            file_path = os.path.join(tmpdirname, filename)
            validated = False

            try:
                # Identify file type
                for key, s3_path in file_mappings.items():
                    if key in filename:
                        expected_schema = EXPECTED_SCHEMAS[key]

                        # Upload first
                        upload_to_s3(file_path, bucket_name, s3_path)

                        # Validate from S3 sample
                        if filename.endswith(".csv"):
                            validate_csv_schema_s3(bucket_name, s3_path, expected_schema)
                        elif filename.endswith(".json"):
                            validate_json_schema_s3(bucket_name, s3_path, expected_schema)

                        uploaded_files.append(f"s3://{bucket_name}/{s3_path}")
                        validated = True
                        break

                if not validated:
                    print(f"⚠️ No schema match found for: {filename}")

            except ValueError as ve:
                # Upload invalid file to error folder
                error_key = f"{error_folder}{filename}"
                upload_to_s3(file_path, bucket_name, error_key)
                print(f"❌ Schema validation failed for {filename}: {ve}")
                print(f"Moved invalid file to: {error_key}")

        if uploaded_files:
            print(f"✅ All valid files uploaded: {uploaded_files}")
            ensure_crawler_exists(crawler_name="scm_bronze_crawler", s3_targets=uploaded_files)
            # trigger_glue_etl(bucket_name, uploaded_files)
            return {"status": "success", "S3_INPUT_PATHS": json.dumps(uploaded_files)}
        else:
            return {"status": "no_valid_files", "message": "No valid files found."}


# ====================== LAMBDA HANDLER ======================
def lambda_handler(event, context=None):
    bucket_name = event['detail']['bucket']['name']
    zip_key = event['detail']['object']['key']
    print("Lambda triggered for:", zip_key)
    result = process_and_upload_zip(bucket_name, zip_key)
    print(f"Lambda Result: {result}")
    return result


# ====================== LOCAL TEST ======================
if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    event_file_path = os.path.join(current_dir, "event_5.json")
    with open(event_file_path, "r") as f:
        event = json.load(f)
    lambda_handler(event)
