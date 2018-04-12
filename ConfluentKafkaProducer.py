#!/usr/bin/python
# -*- coding: UTF-8 -*-
# Created by sshl on 2018/4/12
import sys
from confluent_kafka import Producer

class KafkaProducer():

    def __init__(self, kafkahost, kafkaport):
        self.kafkahost = kafkahost
        self.kafkaport = kafkaport

        conf = {'bootstrap.servers' : '{kafka_host}:{kafka_port}'.format(
        kafka_host=self.kafkahost,
        kafka_port=self.kafkaport),
        'queue.buffering.max.messages' : 100000,
        'queue.buffering.max.ms' : 100,
        'log.connection.close' : False}

        self.Producer = Producer(conf)


    def produceToKafka(self, topic, value):

        def delivery_callback(err, msg):
            if err:
                sys.stderr.write('Message failed delivery: %s\n' % err)
            else:
                sys.stderr.write('Message delivered to %s partition[%d] @ %o\n' %(msg.topic(), msg.partition(), msg.offset()))

        try:
            # Produce value
            self.Producer.produce(topic, value, callback=delivery_callback)

        except BufferError as e:
            sys.stderr.write('Local producer queue is full (%d messages awaiting delivery): try again\n' %len(self.Producer))
        
        self.Producer.poll(0)

        # 可以异步提交无需等待当前produce完成，所以不用flush方法，除非你需要按顺序提交到kafka
        # self.Producer.flush()
