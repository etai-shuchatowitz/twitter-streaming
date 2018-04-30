from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import boto3
import json
import base64
import configparser

firehoseClient = boto3.client('firehose')
configParser = configparser.RawConfigParser()
configFilePath = r'./credentials.txt'
configParser.read(configFilePath)

#Variables that contain the user credentials to access Twitter API
consumer_key = configParser.get('default', 'consumer_key')
consumer_secret = configParser.get('default', 'consumer_secret')
access_token = configParser.get('default', 'access_token')
access_token_secret = configParser.get('default', 'access_token_secret')

print(consumer_key + " " + consumer_secret + " " + access_token + " " + access_token_secret)

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        encodedData = json.dumps(data).encode('utf-8')
        respone = firehoseClient.put_record(
            DeliveryStreamName='twitter-data-stream',
            Record={
                'Data': encodedData})
        return True

    def on_error(self, status):
        print("Error " + str(status))

if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['bitcoin'])
    
