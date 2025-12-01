import boto3
import json

sns_client = boto3.client('sns')

def lambda_handler(event, context):
    message = f"Glue job failed: {json.dumps(event)}"
    response = sns_client.publish(
        TopicArn="arn:aws:sns:ap-south-1:586794480834:GlueJobErrors",
        Message=message,
        Subject="AWS Glue Job Failed"
    )
    return {"status": "Notification sent", "sns_response": response}
