### Python上使用confluent kafka

#### Producer:
```python
from ConfluentKafkaProducer import KafkaProducer

topic = 'test'
msg = 'This is a kafka message'
producer = KafkaProducer("127.0.0.1", "9092")
producer.produceToKafka(topic, msg)
```


#### Consumer:
```python
from ConfluentKafkaConsumer import KafkaConsumer

topics = ["test"]
producer = KafkaConsumer("127.0.0.1", "9092", "consumer_group")
messages = producer.consumeFromKafka(topics)
for msg in messages:
    print msg
```
