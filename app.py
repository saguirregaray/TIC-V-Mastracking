import subprocess
import time
from datetime import datetime

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
app.config['MAIL_USERNAME'] = 'mastracking.um@gmail.com'
app.config['MAIL_PASSWORD'] = 'Mastracking_uy'

# Celery configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

# Alarm configuration
app.config['THRESHOLD'] = 3
app.config['WATER_TANK_THRESHOLD'] = 2
app.config['WATER_TANK_TARGET_TEMP'] = 3

# Density automation
app.config['DENSITY_AUTOMATION'] = True

# Initialize extensions
mail = Mail(app)

# Initialize Celery
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
celery.config_from_object(celeryconfig)

'''EMAIL'''


@cross_origin()
@app.route('/eval_monitor', methods=['get'])
@celery.task(name='tasks.monitor')
def monitor():
    active_processes = db.get_active_processes()
    for process in active_processes:
        evaluate_alarm(process)
        if app.config['DENSITY_AUTOMATION']:
            evaluate_density(process)


def evaluate_density(process):
    try:
        density = process["density"]
        target_density = process['target_density']
        process_id = process["id"]
        current_stage = process["stage"]
        machine_id = get_machine_id(process_id)

        if current_stage == 'fermentation' and density is not None and density <= target_density:
            db.modify_process_stage(process_id, current_stage, 'maduration', machine_id)
    except Exception as e:
        return e.__cause__


def send_async_email_to_list(process_id, description, stage, timestamp):
    """Background task to send an email with Flask-Mail."""
    recipients = db.get_mails()
    recipients_mails = [rec['mail_address'] for rec in recipients]
    msg = Message(subject='Mastracking servicio de alertas: ERROR',
                  sender='mastraking.uy@gmail.com',
                  recipients=recipients_mails)
    msg.body = f"{description}\n" \
               f"\nEl proceso (id: {process_id}) estaba en el estado '{stage}'.\n" \
               f"\nFecha y hora: {timestamp}\n"
    with app.app_context():
        mail.send(msg)


def send_test_email(recipient):
    """Background task to send an email with Flask-Mail."""
    msg = Message(subject='Mastracking servicio de alertas',
                  sender='mastraking.uy@gmail.com',
                  recipients=[recipient])
    msg.body = f"Bienvenido!\n" \
               f"\nMastracking servicio de alertas.\n"
    with app.app_context():
        mail.send(msg)


def evaluate_alarm(process):
    process_id = process['id']
    timestamp = process['timestamp']
    temp = process['current_temperature']
    stage = process['stage']
    target_temp = process['target_temperature']
    water_tank_temperature = db.get_water_tank_temperature(1)['temperature']

    if abs(water_tank_temperature - app.config['WATER_TANK_TARGET_TEMP']) > app.config['WATER_TANK_THRESHOLD']:
        create_water_tank_alert(water_tank_temperature, app.config['WATER_TANK_TARGET_TEMP'], process_id, stage, process)

    if temp is None or abs(temp - target_temp) >= app.config['THRESHOLD'] or (
            (datetime.now() - timestamp).seconds / 3600) > 1:
        create_alert(target_temp, temp, process_id, stage, process)


def create_water_tank_alert(target_temp, water_tank_temperature, process_id, stage, process):
    description = f"ERROR: La temperature objetivo del water_tank era: {target_temp}, pero la actual es: {water_tank_temperature}."
    timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    db.insert_alert(process_id, description, stage, timestamp)

    if (process['alarm_activated'] or
            (datetime.now() - process['alarm_deactivation_timestamp']).seconds / 3600
            > process['alarm_hours_deactivated']):
        send_async_email_to_list(process_id, description, stage, timestamp)


def create_alert(target_temp, temp, process_id, stage, process):
    description = f"ERROR: La temperature objetivo era: {target_temp}, pero la actual es: {temp}."
    timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    db.insert_alert(process_id, description, stage, timestamp)

    if (process['alarm_activated'] or
            (datetime.now() - process['alarm_deactivation_timestamp']).seconds / 3600
            > process['alarm_hours_deactivated']):
        send_async_email_to_list(process_id, description, stage, timestamp)


def get_machine_id(process_id):
    process = db.get_process(process_id)
    stage = process['stage']
    if stage == "fermentation":
        return db.get_fermenter(process['fermenter_id'])['id']
    elif stage == "carbonation":
        return db.get_carbonator(process['carbonator_id'])['id']
    else:
        return db.get_fermenter(process['fermenter_id'])['id']


def get_physical_id(process_id):
    process = db.get_process(process_id)
    stage = process['stage']
    if stage == "fermentation":
        return db.get_fermenter(process['fermenter_id'])['physical_id']
    elif stage == "carbonation":
        return db.get_carbonator(process['carbonator_id'])['physical_id']
    else:
        return db.get_fermenter(process['fermenter_id'])['physical_id']


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
            now = datetime.now()
            name = str(now.year) + str(now.month) + str(now.day)
            state = 1
            stage = 'fermentation'
            fermenter_id = request.json['fermenter_id']
            beer_id = request.json['beer_id']
            return jsonify(result=db.insert_process(fecha_inicio, fecha_fin, stage,
                                                    state, fermenter_id, beer_id, name))
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


@cross_origin()
@app.route('/process/stage', methods=['put'])
def modify_process_stage():
    """
        This method modifies the stage of a process.

        :return: The process records
    """
    try:
        if request.method == 'PUT':
            process_id = request.json["id"]
            current_stage = request.json["current_stage"]
            target_stage = request.json["target_stage"]
            machine_id = request.json["machine_id"]

            if target_stage != 'end':
                physical_id = get_physical_id(process_id)
                subprocess.check_call(
                    ("./resources/config/read_single_temp.sh", str(physical_id)))

            return jsonify(result=db.modify_process_stage(process_id, current_stage, target_stage, machine_id))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/csv/process/active', methods=['get'])
def get_active_processes_csv():
    """
        This method gets all active processes and returns it in csv format.
        :return: The process records
    """
    try:
        if request.method == 'GET':
            return db.get_active_processes_csv()
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/csv/process/temperatures', methods=['post'])
def get_process_temperature_csv():
    """
        This method gets a process record from a given process_id and returns all the asociated temp records.

        :return: The process record's tempratures
    """
    try:
        if request.method == 'POST':
            process_id = request.json["id"]
            return db.get_process_temperature_csv(process_id)
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
            result, status = db.insert_carbonator(name, physical_id)
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
        This method gets the free fermenters records.

        :return: The fermenter records
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
            process_id = request.json['process_id']
            process = db.get_process(process_id)
            physical_id = get_physical_id(process)
            target_temperature = request.json['target_temperature']
            temperature = db.get_temperature_by_process(process_id)['temperature']
            timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            subprocess.check_call(
                ("./resources/config/modify_raspberry_temp.sh", str(physical_id), str(target_temperature)))
            return db.insert_temperature(temperature, timestamp, process_id, target_temperature)
    except Exception as e:
        return e.__cause__


''' DENSITY '''


@cross_origin()
@app.route('/density', methods=['post'])
def insert_density():
    """
        This method receives a process_id and a density from arduino and inserts it into
        the process.

        :return: The status code of the insertion
    """
    try:
        if request.method == 'POST':
            physical_id = request.json['physical_id']
            active_processes = db.get_active_processes()
            process_id = None
            for process in active_processes:
                if process['stage'] == 'fermentation' or process['stage'] == 'maduration':
                    if process['fermenter_physical_id'] == physical_id:
                        process_id = process['id']
                        break
                elif process['carbonator_physical_id'] == physical_id:
                    process_id = process['id']
                    break
            density = request.json['density']
            return jsonify(result=db.insert_density(process_id, density))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/density', methods=['get'])
def get_density():
    """
        This method gets a density value from a given process_id.

        :return: The density value
    """
    try:
        if request.method == 'GET':
            process_id = request.json['process_id']
            return db.get_density(process_id)
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/density-modifier', methods=['put'])
def disable_density_automation():
    """
        This disables the automation for density changes.

        :return: none
    """
    try:
        DENSITY_AUTOMATION = False
    except Exception as e:
        return e.__cause__


def get_physical_id(process):
    stage = process['stage']
    if stage == "fermentation":
        return db.get_fermenter(process['fermenter_id'])['physical_id']
    elif stage == "carbonation":
        return db.get_carbonator(process['carbonator_id'])['physical_id']
    else:
        return db.get_fermenter(process['fermenter_id'])['physical_id']


'''WATER_TANK'''


@cross_origin()
@app.route('/water_tank/temperature', methods=['post'])
def insert_water_tank_temperature():
    """
        This method receives a water_tank_id and inserts a temperature record

        :return: The status code of the insertion
    """
    try:
        if request.method == 'POST':
            water_tank_id = 1
            temperature = request.json['temperature']
            ts = time.time()
            timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            return jsonify(result=db.insert_water_tank_temperature(water_tank_id, temperature, timestamp))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/water_tank/temperature', methods=['get'])
def get_water_tank_temperature():
    """
        This method gets a temperature value from a given water_tank_id.

        :return: The density value
    """
    try:
        if request.method == 'GET':
            return db.get_water_tank_temperature(1)
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


@cross_origin()
@app.route('/csv/alerts', methods=['get'])
def get_alerts_csv():
    """
        This method gets all the alert records.

        :return: The alert records
    """
    try:
        if request.method == 'GET':
            return db.get_alerts_csv()
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/alert/temperature', methods=['post'])
def send_temperature_alert():
    """
      This method receives an alert ping from the raspberry and sends.

      :return: The alert record
  """
    try:
        if request.method == 'POST':
            physical_id = request.json['physical_id']
            active_processes = db.get_active_processes()
            process_id = None
            for process in active_processes:
                if process['stage'] == 'fermentation' or process['stage'] == 'maduration':
                    if process['fermenter_id'] == physical_id:
                        process_id = process['id']
                        break
                elif process['carbonator_id'] == physical_id:
                    process_id = process['id']
                    break

            stage = db.get_process(process_id)['stage']
            timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            description = 'ERROR: No se pudo medir la temperatura correctamente'
            send_async_email_to_list(process_id, description, stage, timestamp)
            return jsonify(result=db.insert_alert(process_id, description, stage, timestamp))
    except Exception as e:
        return e.__cause__


'''MAILS'''


@cross_origin()
@app.route('/mail', methods=['post'])
def insert_mail():
    """
        This method receives the email address and inserts into the db.

        :return: The mail record
    """
    try:
        if request.method == 'POST':
            mail_address = request.json['mail_address']
            send_test_email(mail_address)
            return jsonify(result=db.insert_mail(mail_address))
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/mails', methods=['get'])
def get_mails():
    """
        This method gets all the mail receivers.

        :return: The list of mails.
    """
    try:
        if request.method == 'GET':
            return jsonify(result=db.get_mails())
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/mail', methods=['delete'])
def delete_mail():
    """
        This method mail address and deletes it

        :return: None
    """
    try:
        if request.method == 'DELETE':
            mail_address = request.json['mail_address']
            return jsonify(result=db.delete_mail(mail_address))
    except Exception as e:
        return e.__cause__


'''GRAFICAS'''


@cross_origin()
@app.route('/process/last', methods=['get'])
def get_last_processes():
    """
        This method returns the processes from the last 3 months

        :return: processes from the last 3 months
    """
    try:
        if request.method == 'GET':
            return jsonify(result=db.get_last_processes())
    except Exception as e:
        return e.__cause__


@cross_origin()
@app.route('/process/temperature', methods=['post'])
def get_process_temperature():
    """
        This method returns the temperatures and dates from the given process.

        :return: list of temperatures
    """
    try:
        if request.method == 'POST':
            process_id = request.json['process_id']
            return jsonify(result=db.get_process_temperature_json(process_id))
    except Exception as e:
        return e.__cause__


if __name__ == "__main__":
    app.run(port='8000', ssl_context=('cert.pem', 'key.pem'))
