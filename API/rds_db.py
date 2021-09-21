import pymysql
from API.config import credentials as rds

conn = pymysql.connect(
    host=rds.HOST,
    port=rds.PORT,
    user=rds.USER,
    password=rds.PASSWORD,
    db=rds.DATABASE,
    cursorclass=pymysql.cursors.DictCursor
)

'''TEMPERATURE'''


def insert_temperature(id, temperature, timestamp, process_id):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Temperatures (id, timestamp, temperature, process_id) "
                f"VALUES ({id}, {timestamp}, {temperature}, {process_id})")
    return conn.commit()


def get_temperature(temp_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Temperatures WHERE id = {temp_id}")
    temperature = cur.fetchone()
    return temperature


'''PROCESS'''


def insert_process(id, fecha_inicio, fecha_fin, stage, state, fermenter_id, carbonator_id, beer_id):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Processes (id, fecha_inicio, fecha_finalizacion, stage, state, fermenter_id, "
                f"carbonator_id, beer_id) VALUES ({id}, {fecha_inicio}, {fecha_fin}, '{stage}', '{state}'"
                f", {fermenter_id}, {carbonator_id}, {beer_id})")
    return conn.commit()


def get_process(process_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Processes WHERE id = {process_id}")
    process = cur.fetchone()
    return process


'''BEER'''


def insert_beer(id, name, maduration_temp, fermentation_temp):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Beers (id, name, maduration_temp, fermentation_temp) "
                f"VALUES ({id}, '{name}', {maduration_temp}, {fermentation_temp})")
    return conn.commit()


def get_beer(beer_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Beers WHERE id = {beer_id}")
    beer = cur.fetchone()
    return beer


'''CARBONATOR'''


def insert_carbonator(id, name):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Carbonators (id, name) "
                f"VALUES ({id}, '{name}')")
    return conn.commit()


def get_carbonator(carbonator_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Carbonators WHERE id = {carbonator_id}")
    carbonator = cur.fetchone()
    return carbonator


'''FERMENTER'''


def insert_fermenter(id, name):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Fermenters (id, name) "
                f"VALUES ({id}, '{name}')")
    return conn.commit()


def get_fermenter(fermenter_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Fermenters WHERE id = {fermenter_id}")
    fermenter = cur.fetchone()
    return fermenter
