import kafka_helper
from flask import Flask
import time

producer = kafka_helper.get_kafka_producer()

while True:
    producer.send('truck-actions', key='1NUR622', value={'k':'v'})
    print("I sent something")
    time.sleep(5)
