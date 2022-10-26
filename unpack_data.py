import json
import boto3
import os
import base64

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        dynamo_data = response['Body'].read().decode('utf-8').split("|")
        dynamo_data.pop()
        for tweet in dynamo_data:
            tweet = json.loads(tweet)
            tweet['id'] = int(tweet['id'])
            tweet['tweet'] = base64.b64decode(tweet['tweet'].encode('utf-8')).decode('utf-8')
            table.put_item(Item=tweet)
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e