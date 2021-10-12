import pymysql
from resources.config import credentials as rds

conn = pymysql.connect(
    host=rds.HOST,
    port=rds.PORT,
    user=rds.USER,
    password=rds.PASSWORD,
    db=rds.DATABASE,
    cursorclass=pymysql.cursors.DictCursor
)

'''PROCESS'''


def insert_process(fecha_inicio, fecha_fin, stage, state, fermenter_id, carbonator_id, beer_id):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Processes (fecha_inicio, fecha_finalizacion, stage, state, fermenter_id, "
                f"carbonator_id, beer_id) VALUES ({fecha_inicio}, {fecha_fin}, '{stage}', '{state}'"
                f", {fermenter_id}, {carbonator_id}, {beer_id})")
    conn.commit()
    return get_process(cur.lastrowid)


def get_process(process_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Processes WHERE id = {process_id}")
    process = cur.fetchone()
    return process


def get_processes():
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Processes")
    processes = cur.fetchall()
    return processes


'''BEER'''


def insert_beer(name, maduration_temp, fermentation_temp):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Beers (name, maduration_temp, fermentation_temp) "
                f"VALUES ('{name}', {maduration_temp}, {fermentation_temp})")
    conn.commit()
    return get_beer(cur.lastrowid)


def get_beer(beer_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Beers WHERE id = {beer_id}")
    beer = cur.fetchone()
    return beer


def delete_beer(beer_id):
    cur = conn.cursor()
    cur.execute(f"DELETE FROM Beers WHERE id = {beer_id}")
    beer = cur.fetchone()
    return beer


def get_beers():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Beers")
    beer = cur.fetchall()
    return beer


'''CARBONATOR'''


def insert_carbonator(name):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Carbonators (name) "
                f"VALUES ('{name}')")
    conn.commit()
    return get_carbonator(cur.lastrowid)


def get_carbonator(carbonator_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Carbonators WHERE id = {carbonator_id}")
    carbonator = cur.fetchone()
    return carbonator


def get_carbonators():
    cur = conn.cursor()
    cur.execute(f"SELECT Carbonators.id, Carbonators.name FROM Carbonators "
                f"LEFT JOIN Processes "
                f"ON Carbonators.id = Processes.carbonator_id "
                f"WHERE Processes.carbonator_id IS NULL")
    carbonators = cur.fetchall()
    return carbonators


'''FERMENTER'''


def insert_fermenter(name):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Fermenters (name) "
                f"VALUES ('{name}')")
    conn.commit()
    return get_fermenter(cur.lastrowid)


def get_fermenter(fermenter_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Fermenters WHERE id = {fermenter_id}")
    fermenter = cur.fetchone()
    return fermenter


def get_fermenters():
    cur = conn.cursor()
    cur.execute(f"SELECT Fermenters.id, Fermenters.name FROM Fermenters "
                f"LEFT JOIN Processes "
                f"ON Fermenters.id = Processes.fermenter_id "
                f"WHERE Processes.fermenter_id IS NULL")
    carbonators = cur.fetchall()
    return carbonators


'''TEMPERATURE'''


def insert_temperature(temperature, timestamp, process_id):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Temperatures (timestamp, temperature, process_id) "
                f"VALUES ({timestamp}, {temperature}, {process_id})")
    conn.commit()
    return get_temperature(cur.lastrowid)


def get_temperature(temp_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Temperatures WHERE id = {temp_id}")
    temperature = cur.fetchone()
    return temperature
