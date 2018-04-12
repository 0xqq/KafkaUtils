#!/usr/bin/python
# -*- coding: UTF-8 -*-
# Created by sshl on 2018/4/12
import sys
from confluent_kafka import Consumer, KafkaException, KafkaError

class KafkaConsumer():

	def __init__(self, kafkahost, kafkaport, groupid):
		self.kafkahost = kafkahost
		self.kafkaport = kafkaport
		self.groupid = groupid

		conf = {'bootstrap.servers' : '{kafka_host}:{kafka_port}'.format(
		kafka_host=kafkahost,
		kafka_port=kafkaport),
		'group.id': self.groupid,
		'queue.buffering.max.messages' : 100000,
		'queue.buffering.max.ms' : 100,
		'log.connection.close' : False,
		'session.timeout.ms': 6000,
		'default.topic.config': {'auto.offset.reset': 'smallest'}}

		self.consumer = Consumer(conf)


	def consumeFromKafka(self, topic):

		def print_assignment(consumer, partitions):
			for partition in partitions:
				print('Assignment:', partition)

		# Subscribe to topic
		self.consumer.subscribe(topic, on_assign=print_assignment)

		try:
		    while True:
				# Consume form topic
		        msg = self.consumer.poll(timeout=1.0)
		        if msg is None: continue

		        if msg.error():
		            if msg.error().code() == KafkaError._PARTITION_EOF:
		            	# End of partition event
		                continue
		                # sys.stderr.write('%s partition[%d] reached end at offset %d\n' %(msg.topic(), msg.partition(), msg.offset()))
		            elif msg.error():
		                raise KafkaException(msg.error())
		        else:
		            yield msg.value().decode('utf-8')

		except KeyboardInterrupt:
		    sys.stderr.write('%% Aborted by user KeyboardInterrupt \n')

		self.consumer.close()
