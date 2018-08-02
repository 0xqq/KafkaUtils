#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
from ConfluentKafkaConsumer import KafkaConsumer
import time

consumer = KafkaConsumer("192.168.87.102", "9092", "test")
messages = consumer.consumeFromKafka(["clickLog"])
for message in messages:
	print message