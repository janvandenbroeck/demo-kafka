import kafka_helper
from flask import Flask
import time
import logging

consumer = kafka_helper.get_kafka_consumer(topic='truck-action')

for message in consumer:
    print(message)
    json_record = message.value
    print(json_record['LicensePlate'])
