import time

import pymysql
from flask import Flask, request
from API import rds_db as db

app = Flask(__name__)

'''TEMPERATURE'''


@app.route('/temperature', methods=['post'])
def insert_temperature():
    """
        This method receives the id, temperature and process_id from the frontend,
        creates a new temperature record and inserts it into the RDS database on AWS.

        :return: The status code of the insertion
    """
    try:
        if request.method == 'POST':
            id = request.form['id']
            temperature = request.form['temperature']
            timestamp = time.time()
            process_id = request.form['process_id']
            return str(db.insert_temperature(id, temperature, timestamp, process_id))
    except Exception as e:
        return e.__cause__


@app.route('/temperature', methods=['get'])
def get_temperature():
    """
        This method gets a temperature record from a given process_id.

        :return: The temperature record
    """
    try:
        if request.method == 'GET':
            process_id = request.form['process_id']
            return db.get_temperature(process_id)
    except Exception as e:
        return e.__cause__


'''PROCESS'''


@app.route('/process', methods=['post'])
def insert_process():
    """
       This method receives the id, state, stage, fermenter_id, beer_id and carbonator_id from the frontend,
       creates a new process record and inserts it into the RDS database on AWS.

       :return: The status code of the insertion
   """
    try:
        if request.method == 'POST':
            id = request.form['id']
            fecha_inicio = time.time()
            fecha_fin = pymysql.NULL
            state = request.form['state']  # Esto se deberia setear aca?
            stage = request.form['stage']  # Esto se deberia setear aca?
            fermenter_id = request.form['fermenter_id']
            carbonator_id = request.form['carbonator_id']
            beer_id = request.form['beer_id']
            return str(db.insert_process(id, fecha_inicio, fecha_fin, stage,
                                         state, fermenter_id, carbonator_id, beer_id))
    except Exception as e:
        return e.__cause__


@app.route('/process', methods=['get'])
def get_process():
    """
        This method gets a process record from a given process_id.

        :return: The process record
    """
    try:
        if request.method == 'GET':
            process_id = request.form["id"]
            return db.get_process(process_id)
    except Exception as e:
        return e.__cause__


'''CARBONATOR'''


@app.route('/carbonator', methods=['post'])
def insert_carbonator():
    """
       This method receives the id and name from the frontend,
       creates a new carbonator record and inserts it into the RDS database on AWS.

       :return: The status code of the insertion
   """
    try:
        if request.method == 'POST':
            id = request.form['id']
            name = request.form['name']
            return str(db.insert_carbonator(id, name))
    except Exception as e:
        return e.__cause__


@app.route('/carbonator', methods=['get'])
def get_carbonator():
    """
        This method gets a carbonator record from a given carbonator id.

        :return: The carbonator record
    """
    try:
        if request.method == 'GET':
            carbonator_id = request.form["id"]
            return db.get_carbonator(carbonator_id)
    except Exception as e:
        return e.__cause__


'''FERMENTER'''


@app.route('/fermenter', methods=['post'])
def insert_fermenter():
    """
      This method receives the id and name from the frontend,
      creates a new fermenter record and inserts it into the RDS database on AWS.

      :return: The status code of the insertion
  """
    try:
        if request.method == 'POST':
            id = request.form['id']
            name = request.form['name']
            return str(db.insert_fermenter(id, name))
    except Exception as e:
        return e.__cause__


@app.route('/fermenter', methods=['get'])
def get_fermenter():
    """
        This method gets a fermenter record from a given fermenter id.

        :return: The fermenter record
    """
    try:
        if request.method == 'GET':
            fermenter_id = request.form["id"]
            return db.get_fermenter(fermenter_id)
    except Exception as e:
        return e.__cause__


'''BEER'''


@app.route('/beer', methods=['post'])
def insert_beer():
    """
      This method receives the id, name, maduration temperature and fermentation temperature from the frontend,
      creates a new beer record and inserts it into the RDS database on AWS.

      :return: The status code of the insertion
  """
    try:
        if request.method == 'POST':
            id = request.form['id']
            name = request.form['name']
            maduration_temp = request.form['maduration_temp']
            fermentation_temp = request.form['fermentation_temp']
            return str(db.insert_beer(id, name, maduration_temp, fermentation_temp))
    except Exception as e:
        return e.__cause__


@app.route('/beer', methods=['get'])
def get_beer():
    """
       This method gets a beer record from a given beer id.

       :return: The beer record
   """
    try:
        if request.method == 'GET':
            beer_id = request.form["id"]
            return db.get_beer(beer_id)
    except Exception as e:
        return e.__cause__


if __name__ == "__main__":
    app.run(debug=True)
