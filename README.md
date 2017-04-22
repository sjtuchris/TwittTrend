# TwittTrend
· Use the Kafka service to create a processing queue for the Tweets that are delivered by the Twitter Streaming API. 

· Use Kafka to update the status processing on each tweet so the UI can refresh. 

· Integrate a third party cloud service API into the Tweet processing flow.

# Guide
In console, type in:
  python ..\KafkaProducer.py
  
Then:
  python ..\KafkaWorker.py
  
# Hints:
This consumer is in a consumer group, you can just copy and paste and run multiple consumers in parallel. Make sure you pre-assigned enough partitions before using the scripts. For example, if you open 3 consumers in parallel, you need to have at least 3 partitions in your topic.
