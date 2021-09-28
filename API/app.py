import time

import pymysql
from flask import jsonify
from flask import Flask, request
from API import rds_db as db

app = Flask(__name__)

'''PROCESS'''


@app.route('/process', methods=['post'])
def insert_process():
    """
       This method receives the id, state, stage, fermenter_id, beer_id and carbonator_id from the frontend,
       creates a new process record and inserts it into the RDS database on AWS.

       :return: The process record
   """
    try:
        if request.method == 'POST':
            id = request.json['id']
            fecha_inicio = time.time()
            fecha_fin = pymysql.NULL
            state = request.json['state']  # Esto se deberia setear aca?
            stage = request.json['stage']  # Esto se deberia setear aca?
            fermenter_id = request.json['fermenter_id']
            carbonator_id = request.json['carbonator_id']
            beer_id = request.json['beer_id']
            db.insert_process(id, fecha_inicio, fecha_fin, stage,
                              state, fermenter_id, carbonator_id, beer_id)
            return jsonify(result=db.get_process(id))
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
            process_id = request.json["id"]
            return db.get_process(process_id)
    except Exception as e:
        return e.__cause__


'''CARBONATOR'''


@app.route('/carbonator', methods=['post'])
def insert_carbonator():
    """
       This method receives the id and name from the frontend,
       creates a new carbonator record and inserts it into the RDS database on AWS.

       :return: The carbonator record
   """
    try:
        if request.method == 'POST':
            id = request.json['id']
            name = request.json['name']
            db.insert_carbonator(id, name)
            return jsonify(result=db.get_carbonator(id))
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
            carbonator_id = request.json["id"]
            return db.get_carbonator(carbonator_id)
    except Exception as e:
        return e.__cause__


'''FERMENTER'''


@app.route('/fermenter', methods=['post'])
def insert_fermenter():
    """
      This method receives the id and name from the frontend,
      creates a new fermenter record and inserts it into the RDS database on AWS.

      :return: The fermenter record
  """
    try:
        if request.method == 'POST':
            id = request.json['id']
            name = request.json['name']
            db.insert_fermenter(id, name)
            return jsonify(result=db.get_fermenter(id))
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
            fermenter_id = request.json["id"]
            return db.get_fermenter(fermenter_id)
    except Exception as e:
        return e.__cause__


'''BEER'''


@app.route('/beer', methods=['post'])
def insert_beer():
    """
      This method receives the id, name, maduration temperature and fermentation temperature from the frontend,
      creates a new beer record and inserts it into the RDS database on AWS.

      :return: The beer record.
  """
    try:
        if request.method == 'POST':
            id = request.json['id']
            name = request.json['name']
            maduration_temp = request.json['maduration_temp']
            fermentation_temp = request.json['fermentation_temp']
            db.insert_beer(id, name, maduration_temp, fermentation_temp)
            return jsonify(result=db.get_beer(id))
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
            beer_id = request.json["id"]
            return db.get_beer(beer_id)
    except Exception as e:
        return e.__cause__


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


if __name__ == "__main__":
    app.run(debug=True)
