### Python上使用confluent kafka

#### Producer:
```python
import time
from ConfluentKafkaProducer import KafkaProducer

topic = 'test'
msg = 'This is a kafka message'
producer = KafkaProducer("127.0.0.1", "9092")
while True:
    producer.produceToKafka(topic, msg)
    time.sleep(1)
```


#### Consumer:
```python
from ConfluentKafkaConsumer import KafkaConsumer

topics = ["test"]
consumer = KafkaConsumer("127.0.0.1", "9092", "consumer_group")
messages = consumer.consumeFromKafka(topics)
for msg in messages:
    print msg
```
