from flask import Flask, request, jsonify
import kafka_helper

app = Flask(__name__)
app.producer = kafka_helper.get_kafka_producer()

@app.route('/tx', methods=['POST'])
def post_tx():
    parsed_json = request.get_json()

    #to the debug log
    print(parsed_json)

    licenseplate = parsed_json[0]["LicensePlate"]
    mileagedriven = parsed_json[0]["MileageDriven"]
    fuelconsumed = parsed_json[0]["FuelConsumed"]

    average_consumption_l_100km = (100/mileagedriven) * fuelconsumed

    print("{} {} {}".format(mileagedriven, fuelconsumed, fuelconsumed))

    return "OK {}".format(average_consumption_l_100km)
