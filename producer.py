import kafka_helper
from flask import Flask
import time

producer = kafka_helper.get_kafka_producer()

while True:
    print("will send something")
    producer.send('truck-action', key='1NUR622', value={'k':'v'})
    print("I sent something")
    time.sleep(5)
