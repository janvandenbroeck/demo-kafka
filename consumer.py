import kafka_helper
from sqlalchemy import create_engine, Table, MetaData
import logging
import os

consumer = kafka_helper.get_kafka_consumer(topic='truck-action')
db_engine = create_engine(os.environ['DATABASE_URL'])
metadata = MetaData()
trucks = Table('salesforce.trucks__c', metadata, autoload=True, autoload_with=db_engine)

for message in consumer:
    print(message)
    json_record = message.value

    conn = db_engine.connect()
    result = conn.execute("SELECT licensplate__c FROM salesforce.Truck__c WHERE licenseplate__c = '{}' LIMIT 1".format(json_record['LicensePlate']))

    if len(result) == 0:
        pass

    fuel = result[0]["Fuel__c"] + json_record["FuelConsumed"]
    mileage = result[0]["Mileage__c"] + json_record['MileageDriven']

    average_consumption_l_100km = (100/mileage) * fuel

    print("Average Fuel Consumption {}".format(average_consumption_l_100km))

def update_fuelconsumption(licenseplate, km, fuel):
    pass
