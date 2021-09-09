import time

from flask import Flask, request
from API import rds_db as db

app = Flask(__name__)

'''TEMPERATURE'''


@app.route('/temperature', methods=['post'])
def insert_temperature():
    if request.method == 'POST':
        process_id = request.form['process_id']
        temperature = request.form['temperature']
        timestamp = time.time()
        return db.insert_temperature(process_id, temperature, timestamp)


@app.route('/temperature', methods=['get'])
def get_temperature(process_id):
    if request.method == 'GET':
        return db.get_temperature(process_id)


'''PROCESS'''


@app.route('/process', methods=['post'])
def insert_process():
    if request.method == 'POST':
        fermenter_id = request.form['fermenter_id']
        state = request.form['state']
        stage = request.form['stage']
        timestamp = time.time()
        return db.insert_process(fermenter_id, state, stage, timestamp)


@app.route('/process', methods=['get'])
def get_process(process_id):
    if request.method == 'GET':
        return db.get_process(process_id)


'''CARBONATOR'''


@app.route('/carbonator', methods=['post'])
def insert_carbonator():
    if request.method == 'POST':
        carbonator_id = request.form['carbonator_id']
        beer_id = request.form['beer_id']
        return db.insert_carbonator(carbonator_id, beer_id)


@app.route('/carbonator', methods=['get'])
def get_carbonator(carbonator_id):
    if request.method == 'GET':
        return db.get_carbonator(carbonator_id)


'''FERMENTER'''


@app.route('/fermenter', methods=['post'])
def insert_fermenter():
    if request.method == 'POST':
        fermenter_id = request.form['fermenter_id']
        beer_id = request.form['beer_id']
        return db.insert_fermenter(fermenter_id, beer_id)


@app.route('/fermenter', methods=['get'])
def get_fermenter(fermenter_id):
    if request.method == 'GET':
        return db.get_fermenter(fermenter_id)


'''BEER'''


@app.route('/beer', methods=['post'])
def insert_beer():
    if request.method == 'POST':
        beer_id = request.form['beer_id']
        description = request.form['description']
        return db.insert_beer(beer_id, description)


@app.route('/beer', methods=['get'])
def get_beer(beer_id):
    if request.method == 'GET':
        return db.get_beer(beer_id)


if __name__ == "__main__":
    app.run(debug=True)
