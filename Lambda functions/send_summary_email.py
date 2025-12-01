import json

# def lambda_handler(event, context):
#     # TODO implement
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }
import boto3
import time
import io
import csv
from botocore.exceptions import ClientError

# ====== CONFIG ======
ATHENA_DATABASE = "scm_gold"
ATHENA_OUTPUT = "s3://scm-analytics-2025/athena/queryResults/"
QUERY_STRING = """
SELECT 
  sales_date,
  supplier_id,
  supplier_name,
  total_sales_amount,
  total_quantity_sold,
  total_orders,
  last_updated
FROM scm_gold.fact_sales_summary_by_supplier_daily WHERE supplier_id IS NOT NULL
ORDER BY sales_date DESC
LIMIT 10;
"""
SENDER_EMAIL = "azhagans128@gmail.com"
RECEIVER_EMAIL = "azhagans128@gmail.com"
REGION = "ap-southeast-1"

# ====== STEP 1: Run Athena Query ======
def run_athena_query():
    athena = boto3.client('athena')
    response = athena.start_query_execution(
        QueryString=QUERY_STRING,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    query_execution_id = response['QueryExecutionId']

    # Wait for query to complete
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state != 'SUCCEEDED':
        raise Exception(f"Query failed with state: {state}")

    return query_execution_id

# ====== STEP 2: Get Results and Format HTML Table ======
def get_results_html(query_execution_id):
    athena = boto3.client('athena')
    results_paginator = athena.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(QueryExecutionId=query_execution_id)

    rows = []
    for page in results_iter:
        for row in page['ResultSet']['Rows']:
            rows.append([col.get('VarCharValue', '') for col in row['Data']])

    headers = rows[0]
    data_rows = rows[1:]

    table_html = """
    <html>
    <head>
      <style>
        table {{ border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
      </style>
    </head>
    <body>
      <h2>📊 Sales Supplier Summary</h2>
      <table>
        <tr>{}</tr>
    """.format("".join(f"<th>{h}</th>" for h in headers))

    for row in data_rows:
        table_html += "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"

    table_html += "</table></body></html>"

    return table_html

# ====== STEP 3: Send HTML Email ======
def send_email(html_body):
    ses = boto3.client('ses', region_name=REGION)
    try:
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={'ToAddresses': [RECEIVER_EMAIL]},
            Message={
                'Subject': {'Data': 'Daily Sales Supplier Summary Report'},
                'Body': {'Html': {'Data': html_body}}
            }
        )
        print("✅ Email sent! Message ID:", response['MessageId'])
    except ClientError as e:
        print("❌ Email failed:", e.response['Error']['Message'])

# ====== MAIN ======
def main():
    print("⏳ Running Athena query...")
    query_id = run_athena_query()
    print("✅ Query completed.")

    print("📄 Formatting results as HTML...")
    html = get_results_html(query_id)

    print("📧 Sending email...")
    send_email(html)


def lambda_handler(event, context):
    query_id = run_athena_query()
    html = get_results_html(query_id)
    send_email(html)
    return {"statusCode": 200, "body": "Email sent with Athena report."}


if __name__ == "__main__":
    main()
