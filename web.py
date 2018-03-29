import os

from flask import Flask, request, jsonify, send_file
from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.sql.expression import func

import kafka_helper

app = Flask(__name__)
app.producer = kafka_helper.get_kafka_producer()

app.db_engine = create_engine(os.environ['DATABASE_URL'])
app.metadata = MetaData(bind=app.db_engine)

@app.route('/tx', methods=['POST'])
def post_tx():

    if request.headers['Authorization'].split(' ')[1] != os.environ['BEARER']:
        return "Bad Request", 400

    parsed_json = request.get_json()[0]

    #to the debug log
    print(parsed_json)

    licenseplate = parsed_json["LicensePlate"]
    mileagedriven = parsed_json["MileageDriven"]
    fuelconsumed = parsed_json["FuelConsumed"]

    app.producer.send('truck-action', key=bytes(licenseplate), value=parsed_json)
    return "OK"

@app.route('/products')
def get_products():
    print('Getting them products!')
    conn = app.db_engine.connect()
    result = conn.execute("SELECT * FROM salesforce.product2")
    result_set = []
    for r in result:
        row = dict()
        for key, value in r.items():
            row[key] = value
        result_set.append(row)

    return jsonify(result_set)
