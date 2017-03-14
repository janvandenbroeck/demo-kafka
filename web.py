import os

from flask import Flask, request, jsonify
import kafka_helper

app = Flask(__name__)
app.producer = kafka_helper.get_kafka_producer()

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
    average_consumption_l_100km = (100/mileagedriven) * fuelconsumed

    app.producer.send('truck-action', key=bytes(licenseplate), value=parsed_json)
    return "OK"
