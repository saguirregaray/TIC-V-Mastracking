import time

import pymysql
from flask import Flask, request
from API import rds_db as db

app = Flask(__name__)

'''TEMPERATURE'''


@app.route('/temperature', methods=['post'])
def insert_temperature():
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
    try:
        if request.method == 'GET':
            process_id = request.form['process_id']
            return db.get_temperature(process_id)
    except Exception as e:
        return e.__cause__


'''PROCESS'''


@app.route('/process', methods=['post'])
def insert_process():
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
    try:
        if request.method == 'GET':
            process_id = request.form["id"]
            return db.get_process(process_id)
    except Exception as e:
        return e.__cause__


'''CARBONATOR'''


@app.route('/carbonator', methods=['post'])
def insert_carbonator():
    try:
        if request.method == 'POST':
            id = request.form['id']
            name = request.form['name']
            return str(db.insert_carbonator(id, name))
    except Exception as e:
        return e.__cause__


@app.route('/carbonator', methods=['get'])
def get_carbonator():
    try:
        if request.method == 'GET':
            carbonator_id = request.form["id"]
            return db.get_carbonator(carbonator_id)
    except Exception as e:
        return e.__cause__


'''FERMENTER'''


@app.route('/fermenter', methods=['post'])
def insert_fermenter():
    try:
        if request.method == 'POST':
            id = request.form['id']
            name = request.form['name']
            return str(db.insert_fermenter(id, name))
    except Exception as e:
        return e.__cause__


@app.route('/fermenter', methods=['get'])
def get_fermenter():
    try:
        if request.method == 'GET':
            fermenter_id = request.form["id"]
            return db.get_fermenter(fermenter_id)
    except Exception as e:
        return e.__cause__


'''BEER'''


@app.route('/beer', methods=['post'])
def insert_beer():
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
    try:
        if request.method == 'GET':
            beer_id = request.form["id"]
            return db.get_beer(beer_id)
    except Exception as e:
        return e.__cause__


if __name__ == "__main__":
    app.run(debug=True)
