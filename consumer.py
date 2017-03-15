import kafka_helper
from sqlalchemy import create_engine, Table, MetaData
import logging
import os

consumer = kafka_helper.get_kafka_consumer(topic='truck-action')
db_engine = create_engine(os.environ['DATABASE_URL'])
metadata = MetaData(bind=db_engine)
trucks = Table('truck__c', metadata, autoload=True, schema='salesforce')

def update_fuelconsumption(licenseplate, km, fuel):
    conn = db_engine.connect()
    result = conn.execute("SELECT licenseplate__c, fuel__c, mileage__c FROM salesforce.truck__c WHERE licenseplate__c = '{}' LIMIT 1".format(licenseplate))
    result = result.first()

    if not result:
        pass

    print(result)
    fuel = result["fuel__c"] + fuel
    mileage = result["mileage__c"] + km

    average_consumption_l_100km = (100/mileage) * fuel

    result = conn.execute("UPDATE salesforce.truck__c SET average_consumption__c = {}, fuel__c = {}, mileage = {} WHERE licenseplate__c = '{}'".format(average_consumption_l_100km, fuel, mileage, licenseplate))

    print("New Average Fuel Consumption {}".format(average_consumption_l_100km))

    conn.close()


for message in consumer:
    print(message)
    json_record = message.value

    update_fuelconsumption(json_record["LicensePlate"], json_record['MileageDriven'], json_record["FuelConsumed"])
