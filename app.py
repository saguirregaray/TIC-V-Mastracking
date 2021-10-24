import time

import pymysql
from celery import Celery
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
from flask_mail import Mail, Message

import rds_db as db
from resources.config import celeryconfig

app = Flask(__name__)
app.config['SECRET_KEY'] = '1234567'

# CORS configuration
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

# Flask-Mail configuration
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
app.config['MAIL_USERNAME'] = 'mastraking.uy@gmail.com'
app.config['MAIL_PASSWORD'] = 'mastracking_uy'

# Celery configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

# Initialize extensions
mail = Mail(app)

# Initialize Celery
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
celery.config_from_object(celeryconfig)

'''EMAIL'''


@celery.task(name='tasks.email')
def send_async_email():
    """Background task to send an email with Flask-Mail."""
    msg = Message(subject='MasTracking',
                  sender='mastraking.uy@gmail.com',
                  recipients=['seraguirregaray@gmail.com'])
    msg.body = 'Mastracking alert service: There was an error'
    with app.app_context():
        mail.send(msg)


'''PROCESS'''


@cross_origin()
@app.route('/process', methods=['post'])
def insert_process():
    """
       This method receives the id, state, stage, fermenter_id, beer_id and carbonator_id from the frontend,
       creates a new process record and inserts it into the RDS database on AWS.

       :return: The process record
   """
    try:
        if request.method == 'POST':
            fecha_inicio = time.time()
            fecha_fin = pymysql.NULL
            state = request.json['state']  # Esto se deberia setear aca?
            stage = request.json['stage']  # Esto se deberia setear aca?
            fermenter_id = request.json['fermenter_id']
            carbonator_id = request.json['carbonator_id']
            beer_id = request.json['beer_id']
            return jsonify(result=db.insert_process(fecha_inicio, fecha_fin, stage,
                                                    state, fermenter_id, carbonator_id, beer_id))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/process', methods=['get'])
def get_process():
    """
        This method gets a process record from a given process_id.

        :return: The process record
    """
    try:
        if request.method == 'GET':
            process_id = request.json["id"]
            return db.get_process(process_id)
    except Exception as e:
        return e.__cause__


'''CARBONATOR'''


@cross_origin()
@app.route('/carbonator', methods=['post'])
def insert_carbonator():
    """
       This method receives the id and name from the frontend,
       creates a new carbonator record and inserts it into the RDS database on AWS.

       :return: The carbonator record
   """
    try:
        if request.method == 'POST':
            name = request.json['name']
            return jsonify(result=db.insert_carbonator(name))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/carbonator', methods=['get'])
def get_carbonator():
    """
        This method gets a carbonator record from a given carbonator id.

        :return: The carbonator record
    """
    try:
        if request.method == 'GET':
            carbonator_id = request.json["id"]
            return db.get_carbonator(carbonator_id)
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/carbonators', methods=['get'])
def get_carbonators():
    """
        This method gets all the carbonator records.

        :return: The carbonator records
    """
    try:
        if request.method == 'GET':
            return jsonify(result=db.get_carbonators())
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/free_carbonators', methods=['get'])
def get_free_carbonators():
    """
        This method gets the free carbonator records.

        :return: The carbonator records
    """
    try:
        if request.method == 'GET':
            return jsonify(result=db.get_free_carbonators())
    except Exception as e:
        return e.__cause__


'''FERMENTER'''


@cross_origin()
@app.route('/fermenter', methods=['post'])
def insert_fermenter():
    """
      This method receives the id and name from the frontend,
      creates a new fermenter record and inserts it into the RDS database on AWS.

      :return: The fermenter record
  """
    try:
        if request.method == 'POST':
            name = request.json['name']
            return jsonify(result=db.insert_fermenter(name))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/fermenter', methods=['get'])
def get_fermenter():
    """
        This method gets all the free fermenter records.

        :return: The fermenter records
    """
    try:
        if request.method == 'GET':
            return db.get_fermenters()
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/fermenters', methods=['get'])
def get_fermenters():
    """
        This method gets all the fermenters records.

        :return: The carbonator records
    """
    try:
        if request.method == 'GET':
            return jsonify(result=db.get_fermenters())
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/free_fermenters', methods=['get'])
def get_free_fermenters():
    """
        This method gets the free fermenter records.

        :return: The carbonator records
    """
    try:
        if request.method == 'GET':
            return jsonify(result=db.get_free_fermenters())
    except Exception as e:
        return e.__cause__


'''BEER'''


@cross_origin()
@app.route('/beer', methods=['post'])
def insert_beer():
    """
      This method receives the id, name, maduration temperature and fermentation temperature from the frontend,
      creates a new beer record and inserts it into the RDS database on AWS.

      :return: The beer record.
  """
    try:
        if request.method == 'POST':
            name = request.json['name']
            maduration_temp = request.json['maduration_temp']
            fermentation_temp = request.json['fermentation_temp']
            return jsonify(result=db.insert_beer(name, maduration_temp, fermentation_temp))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/beer', methods=['get'])
def get_beer():
    """
       This method gets a beer record from a given beer id.

       :return: The beer record
   """
    try:
        if request.method == 'GET':
            beer_id = request.json["id"]
            return db.get_beer(beer_id)
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/beers', methods=['get'])
def get_beers():
    """
       This method returns a list with all the beers.

       :return: The list of beer records
   """
    try:
        if request.method == 'GET':
            return jsonify(result=db.get_beers())
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/beer', methods=['delete'])
def delete_beer():
    """
       This method deletes a beer given an id.

       :return: None
   """
    try:
        if request.method == 'DELETE':
            beer_id = request.json["id"]
            return jsonify(result=db.delete_beer(beer_id))
    except Exception as e:
        return e.__cause__


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
    app.run(debug=True)
