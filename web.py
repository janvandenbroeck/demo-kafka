from flask import Flask, request, jsonify
import kafka_helper

app = Flask(__name__)
app.producer = kafka_helper.get_kafka_producer()

@app.route('/tx', methods=['POST'])
def post_tx():
    parsed_json = request.get_json()
    print(len(parsed_json))

    print('LicensePlate = {}'.format(parsed_json[0]["LicensePlate"]))

    return "OK"
