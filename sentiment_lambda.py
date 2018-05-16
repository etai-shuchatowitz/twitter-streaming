import os
import boto3
import json
import pprint

comprehend = boto3.client('comprehend')

def lambda_handler(event, context):

    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        data = response['data']
        for line in data:
            tweet = json.loads(line)
            language = tweet['user']['lang']
            if language == 'en':
                text = tweet['text']
                timestamp = tweet['timestamp_ms']
                followers_count = tweet['user']['followers_count']
                response = comprehend.detect_sentiment(
                    Text=text,
                    LanguageCode='en'
                )
                sentiment = response['SentimentScore']
            
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
