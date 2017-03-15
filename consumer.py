import kafka_helper
from sqlalchemy import create_engine, Table, MetaData
import logging
import os

consumer = kafka_helper.get_kafka_consumer(topic='truck-action')
db_engine = create_engine(os.environ['DATABASE_URL'])
metadata = MetaData(bind=db_engine)
#trucks = Table('salesforce.truck__c', metadata, autoload=True, autoload_with=db_engine)

for message in consumer:
    print(message)
    json_record = message.value

    conn = db_engine.connect()
    result = conn.execute("SELECT * FROM salesforce.truck__c WHERE licenseplate__c = '{}' LIMIT 1".format(json_record['LicensePlate']))

    fuel = result[0]["Fuel__c"] + json_record["FuelConsumed"]
    mileage = result[0]["Mileage__c"] + json_record['MileageDriven']

    average_consumption_l_100km = (100/mileage) * fuel

    print("Average Fuel Consumption {}".format(average_consumption_l_100km))

def update_fuelconsumption(licenseplate, km, fuel):
    pass
