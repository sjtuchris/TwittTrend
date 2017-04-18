from textblob import TextBlob
import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
# consumer = KafkaConsumer('twitterstream',value_deserializer=lambda m: json.loads(m.decode('ascii')),bootstrap_servers='localhost:2181')
consumer = KafkaConsumer('workers',bootstrap_servers=['34.208.177.202:9092'],group_id='workergroup')
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         bootstrap_servers='34.208.177.202:9092')
mytopic = 'twitterstream'  # ex. 'twitterstream', or 'test' ...
poscount = 0
negcount = 0
neucount = 0

print 'Connected!'
# consumer = KafkaConsumer('twitterstream',value_deserializer=lambda m: json.loads(m.decode('ascii')),bootstrap_servers='34.208.177.202:2181')
while True:
    try:
        for msg in consumer:
        	# print json.loads(msg.value)['coordinates']
        	# print json.dumps(json.loads(msg.value), indent=4, sort_keys=True)
            data1 = json.loads(msg.value)

            coordinates = data1["coordinates"]["coordinates"]
            timestamp_ms = data1["timestamp_ms"]
            id_str = data1["id_str"]
            text = data1["text"]
            screen_name = data1["user"]["screen_name"]
            profile_image_url_https = data1["user"]["profile_image_url_https"]
            # Sentiment analysis
            testimonial = TextBlob(text)

            if testimonial.sentiment.polarity < 0:
                label = "negative" 
                negcount = negcount+1
            elif testimonial.sentiment.polarity > 0:
                label = "positive"
                poscount = poscount+1
            else:
                label = "neutral"
                neucount = neucount+1
            # rebuild data
            data2 = {
                "coordinates": {"coordinates": coordinates},
                "id_str": id_str,
                "timestamp_ms": timestamp_ms,
                "text": text,
                "user": {
                    "id_str": id_str,
                    "profile_image_url_https": profile_image_url_https,
                    "screen_name": screen_name
                },
                "label": label}
            # Push into kafka
            producer.send(mytopic, data2)
            # if poscount>50:
            #     print poscount,negcount,neucount
            #     poscount=0
            #     negcount=0
            #     neucount=0
            #     print data2["label"],testimonial.sentiment.polarity,text
    except Exception, e:
        print e
        continue
