import tweepy
from httplib import IncompleteRead
import requests,json
import json
import threading, logging, time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
from kafka import KafkaProducer
import string


######################################################################
# Authentication details. To  obtain these visit dev.twitter.com
######################################################################

consumer_key = 'wCgyjfYFUGuwF4j3gb19qP9gi'  # eWkgf0izE2qtN8Ftk5yrVpaaI
consumer_secret = 'B8GDdXgifHuep5OtdAZMVhbzKsuaO22bhnFRUu9Qj4lbGy2Emy'  # BYYnkSEDx463mGzIxjSifxfXN6V1ggpfJaGBKlhRpUMuQ02lBX
access_token = '2985268217-ajEuNG3Z04VD16r31najRn0UrBAgNUFdJVaJJmC'  # 1355650081-Mq5jok7mbcrIbTpqZPcMHgWjcymqSrG1kVaut39
access_token_secret = '9HgLLjlYcYhQDAI5JHjGxOcz2hCDrCUQDFK2TXFY7gx6g'  # QovqxQnw0hSPrKwFIYLWct3Zv4MeGMash66IaOoFyXNWs

######################################################################
# Authentication details to monkeylearn
######################################################################


mytopic = 'workers'  # ex. 'twitterstream', or 'test' ...
# producer = KafkaProducer(bootstrap_servers='localhost:9092')
# producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),bootstrap_servers='localhost:9092')
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         bootstrap_servers='34.208.177.202:9092')

print "Connected!"


######################################################################
# Create a handler for the streaming data that stays open...
######################################################################

class StdOutListener(tweepy.StreamListener):


    ######################################################################
    # For each status event
    ######################################################################




    def on_data(self, data):

        # Some data are lack of coordinates
        try:
            if json.loads(data)['coordinates'] is None:
                return True
        except Exception, e:
            return True

        # print json.loads(data)['coordinates']


        data1 = json.loads(data)


        # Push into kafka
        producer.send(mytopic, data1)
        print data1["id_str"]
        # print json.dumps(data2, indent=4, sort_keys=True)
        # except Exception, e:
        #
        #     print 'Not sent!'
        #     return True

        return True

    def on_error(self, status_code):

        print('Got an error with status code: ' + str(status_code))
        return True  # To continue listening

    def on_timeout(self):

        print('Timeout...')
        return True  # To continue listening


######################################################################
# Main Loop Init
######################################################################


if __name__ == '__main__':
    # print(u','.join(['%.4f' % l for l in [-180.0,-90,180,90]]))
    listener = StdOutListener()

    # sign oath cert

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

    auth.set_access_token(access_token, access_token_secret)

    # uncomment to use api in stream for data send/retrieve algorythms
    # api = tweepy.API(auth)
    counter = 0
    while True:
        try:
            stream = tweepy.Stream(auth, listener).filter(languages=["en"], locations=[-180.0, -90.0, 180.0, 90.0])

        except Exception, e:
            # twitter filter is unstable, avoid interruption
            continue
