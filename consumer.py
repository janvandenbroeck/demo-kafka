import kafka_helper
from sqlalchemy import create_engine, Table, MetaData
from pymongo import MongoClient
import logging
import os

# Postgres setup
consumer = kafka_helper.get_kafka_consumer(topic='truck-action')
db_engine = create_engine(os.environ['DATABASE_URL'])
metadata = MetaData(bind=db_engine)
trucks = Table('truck__c', metadata, autoload=True, schema='salesforce')

# Mongo setup
client = MongoClient(os.environ['MONGODB_URI'])
mongo_db = client.heroku_2pj49ssl

def update_fuelconsumption(licenseplate, km, fuel):

    # getting the data
    conn = db_engine.connect()
    result = conn.execute("SELECT licenseplate__c, fuel__c, mileage__c, phone__c FROM salesforce.truck__c WHERE licenseplate__c = '{}' LIMIT 1".format(licenseplate)).first()

    if not result:
        ins = trucks.insert().values(licenseplate__c=licenseplate)
        conn.execute(ins)
        result = conn.execute("SELECT licenseplate__c, fuel__c, mileage__c, phone__c FROM salesforce.truck__c WHERE licenseplate__c = '{}' LIMIT 1".format(licenseplate)).first()

    # Calculating the new values
    fuel = result["fuel__c"] + fuel
    mileage = result["mileage__c"] + km
    average_consumption_l_100km = (100/mileage) * fuel

    if average_consumption_l_100km > 25:
        r = requests.post(os.environ['BLOWERIO_URL'] + '/messages', data={'to': result["phone__c"], 'message': "Watch out, truck with licenseplate {} has a high fuel consumption".format(result['licenseplate__c'])})

    # Updating the record
    result = conn.execute("UPDATE salesforce.truck__c SET average_consumption__c = {}, fuel__c = {}, mileage__c = {} WHERE licenseplate__c = '{}'".format(average_consumption_l_100km, fuel, mileage, licenseplate))
    conn.close()

    print("New Average Fuel Consumption {}".format(average_consumption_l_100km))

def write_to_mongo(json_msg):
    truck_actions = mongo_db.truck_actions
    inserted_id = truck_actions.insert_one(json_msg).inserted_id
    print(inserted_id)

for message in consumer:
    print(message)
    json_record = message.value

    update_fuelconsumption(json_record["LicensePlate"], json_record['MileageDriven'], json_record["FuelConsumed"])
    write_to_mongo(json_record)
