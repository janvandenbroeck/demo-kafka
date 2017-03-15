import kafka_helper
from sqlalchemy import create_engine, Table, MetaData
import logging
import os

consumer = kafka_helper.get_kafka_consumer(topic='truck-action')
db_engine = create_engine(os.environ['DATABASE_URL'])
metadata = MetaData(bind=db_engine)
trucks = Table('truck__c', metadata, autoload=True, schema='salesforce')

for message in consumer:
    print(message)
    json_record = message.value

    conn = db_engine.connect()
    result = conn.execute("SELECT licenseplate__c, fuel__c, mileage__c FROM salesforce.truck__c WHERE licenseplate__c = '{}' LIMIT 1".format(json_record['LicensePlate']))
    result = result.first()

    if not result:
        pass

    print(result)
    fuel = result["Fuel__c"] + json_record["FuelConsumed"]
    mileage = result["Mileage__c"] + json_record['MileageDriven']

    average_consumption_l_100km = (100/mileage) * fuel

    print("New Average Fuel Consumption {}".format(average_consumption_l_100km))

def update_fuelconsumption(licenseplate, km, fuel):
    pass
