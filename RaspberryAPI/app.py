import time

from flask import jsonify
from flask import Flask, request
from RaspberryAPI import rds_db as db
from flask_cors import CORS, cross_origin

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

'''TEMPERATURE'''


@cross_origin()
@app.route('/temperature', methods=['post'])
def insert_temperature():
    """
        This method receives the id, temperature and process_id from the frontend,
        creates a new temperature record and inserts it into the RDS database on AWS.

        :return: The status code of the insertion
    """
    try:
        if request.method == 'POST':
            temperature = request.json['temperature']
            timestamp = time.time()
            process_id = request.json['process_id']
            return jsonify(result=db.insert_temperature(temperature, timestamp, process_id))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/temperature', methods=['get'])
def get_temperature():
    """
        This method gets a temperature record from a given process_id.

        :return: The temperature record
    """
    try:
        if request.method == 'GET':
            process_id = request.json['id']
            return db.get_temperature(process_id)
    except Exception as e:
        return e.__cause__


if __name__ == "__main__":
    app.run(debug=True, port=5001)
