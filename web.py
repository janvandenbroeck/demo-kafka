from flask import Flask, request, jsonify
import kafka_helper

app = Flask(__name__)
app.producer = kafka_helper.get_kafka_producer()

@app.route('/tx', methods=['POST'])
def post_tx():
    parsed_json = request.get_json()[0]

    #to the debug log
    print(parsed_json)

    licenseplate = parsed_json["LicensePlate"]
    mileagedriven = parsed_json["MileageDriven"]
    fuelconsumed = parsed_json["FuelConsumed"]
    average_consumption_l_100km = (100/mileagedriven) * fuelconsumed

    if licenseplate == "1NUR622":
        app.producer.send('truck-action', key='1NUR622', value=parsed_json)
        return "OK"
    else:
        return "Bad Request", 400
