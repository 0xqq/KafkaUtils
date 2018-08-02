#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
from ConfluentKafkaProducer import KafkaProducer
import time

topic = 'clickLog'
msg = ''
producer = KafkaProducer("192.168.87.102", "9092")
with(open("../resource/access.log", "rt")) as fp:
	for line in fp:
		producer.produceToKafka(topic, line.rstrip())
		time.sleep(1)
