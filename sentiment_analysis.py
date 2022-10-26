import boto3
import os

comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])
    
    for record in event['Records']:
        if record['eventName'] == "INSERT":
            tweet_text = record['dynamodb']['NewImage']['tweet']['S']
            response = comprehend.detect_sentiment(
                Text=tweet_text,
                LanguageCode='en'
            )
            
            table.update_item(
                Key={
                    "sentiment": response['Sentiment']
                },
                UpdateExpression="ADD tweets :val",
                ConditionExpression="attribute_not_exists(sentiment) OR sentiment = :sentiment",
                ExpressionAttributeValues={
                    ":val":1,
                    ":sentiment":response['Sentiment']
                },
                ReturnValues='UPDATED_NEW'
            )
            