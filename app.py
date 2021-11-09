import time

import pymysql
from celery import Celery
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
from flask_mail import Mail, Message
from datetime import datetime

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
app.config['MAIL_USERNAME'] = 'mastracking.um@gmail.com'
app.config['MAIL_PASSWORD'] = 'Mastracking_uy'

# Celery configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

# Alarm configuration
app.config['THRESHOLD'] = 3

# Initialize extensions
mail = Mail(app)

# Initialize Celery
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
celery.config_from_object(celeryconfig)

'''EMAIL'''


@celery.task(name='tasks.monitor')
def monitor():
    active_processes = db.get_active_processes()
    for process in active_processes:
        evaluate_alarm(process)


def send_async_email(process_id, description, stage, timestamp):
    """Background task to send an email with Flask-Mail."""
    msg = Message(subject='Mastracking alert service: There was an error',
                  sender='mastraking.uy@gmail.com',
                  recipients=['seraguirregaray@gmail.com'])
    msg.body = f"{description}\n" \
               f"\nThe process (id: {process_id}) was in the '{stage}' stage.\n" \
               f"\nDate and time: {timestamp}\n"
    with app.app_context():
        mail.send(msg)


def evaluate_alarm(process):
    process_id = process['id']
    temp = process['current_temperature']
    stage = process['stage']
    target_temp = process['target_temperature']

    if temp is None or abs(temp - target_temp) >= app.config['THRESHOLD']:
        create_alert(target_temp, temp, process_id, stage, process)


def create_alert(target_temp, temp, process_id, stage, process):
    description = f"ERROR: The target temperature was: {target_temp} and the actual temp is: {temp}."
    timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    db.insert_alert(process_id, description, stage, timestamp)

    if (process['alarm_activated'] or
            (datetime.now() - process['alarm_deactivation_timestamp']).seconds / 3600
            > process['alarm_hours_deactivated']):
        send_async_email(process_id, description, stage, timestamp)


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
            ts = time.time()
            fecha_inicio = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            fecha_fin = pymysql.NULL
            state = 1 
            stage = 'fermentation'
            fermenter_id = request.json['fermenter_id']
            beer_id = request.json['beer_id']
            return jsonify(result=db.insert_process(fecha_inicio, fecha_fin, stage,
                                                    state, fermenter_id, beer_id))
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
            return jsonify(db.get_process(process_id))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/process/active', methods=['get'])
def get_active_processes():
    """
        This method gets all active processes.
        :return: The process records
    """
    try:
        if request.method == 'GET':
            return jsonify(db.get_active_processes())
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
            physical_id = request.json['physical_id']
            result, status =db.insert_carbonator(name, physical_id)
            return jsonify(result=result, status=status)
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
@app.route('/carbonator', methods=['delete'])
def delete_carbonator():
    """
       This method deletes a carbonator given an id.

       :return: None
   """
    try:
        if request.method == 'DELETE':
            carbonator_id = request.json["id"]
            return jsonify(result=db.delete_carbonator(carbonator_id))
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
            physical_id = request.json['physical_id']
            result, status = db.insert_fermenter(name, physical_id)
            return jsonify(result=result, status=status)
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
@app.route('/fermenter', methods=['delete'])
def delete_fermenter():
    """
       This method deletes a fermenter given an id.

       :return: None
   """
    try:
        if request.method == 'DELETE':
            fermenter_id = request.json["id"]
            return jsonify(result=db.delete_fermenter(fermenter_id))
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
            timestamp = request.json['timestamp']
            target_temperature = request.json['target_temperature']
            temperature = request.json['temperature']
            process_id = request.json['process_id']
            return jsonify(result=db.insert_temperature(temperature, timestamp, process_id, target_temperature))
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


@cross_origin()
@app.route('/temperature', methods=['put'])
def modify_temperature():
    """
        This method gets a temperature record from a given process_id and modifies the target temperature

        :return: The temperature record
    """
    try:
        if request.method == 'PUT':
            temp_id = request.json['id']
            target_temperature = request.json['target_temperature']
            return db.modify_target_temp(temp_id, target_temperature)
    except Exception as e:
        return e.__cause__


'''ALERTS'''


@cross_origin()
@app.route('/alert', methods=['post'])
def insert_alert():
    """
      This method receives a description of the alert and creates a new instance with the corresponding timestamp.

      :return: The alert record
  """
    try:
        if request.method == 'POST':
            process_id = request.json['process_id']
            stage = request.json['stage']
            ts = time.time()
            timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            description = request.json['description']
            return jsonify(result=db.insert_alert(process_id, description, stage, timestamp))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/alert/deactivate', methods=['put'])
def deactivate_alert():
    """
      This method receives process id and an amount of hours, and deactivates the alarm for the given process for
      the given time.

      :return: The process record with the deactivated alarm
  """
    try:
        if request.method == 'PUT':
            process_id = request.json['process_id']
            alarm_hours_deactivated = request.json['alarm_hours_deactivated']
            ts = time.time()
            alarm_deactivation_timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            alarm_activated = False
            return jsonify(result=db.deactivate_alert(process_id, alarm_deactivation_timestamp, alarm_hours_deactivated,
                                                      alarm_activated))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/alert/activate', methods=['put'])
def activate_alert():
    """
      This method receives process id  and activates the alarm for the given process.

      :return: The process record with the deactivated alarm
  """
    try:
        if request.method == 'PUT':
            process_id = request.json['process_id']
            alarm_activated = True
            return jsonify(result=db.activate_alert(process_id, alarm_activated))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/alert', methods=['get'])
def get_alert():
    """
        This method gets an alert given an id.

        :return: The alert record.
    """
    try:
        if request.method == 'GET':
            id = request.json['id']
            return db.get_alert(id)
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/alerts', methods=['get'])
def get_alerts():
    """
        This method gets all the alert records.

        :return: The alert records
    """
    try:
        if request.method == 'GET':
            return jsonify(result=db.get_alerts())
    except Exception as e:
        return e.__cause__


if __name__ == "__main__":
    app.run(host='0.0.0.0')
