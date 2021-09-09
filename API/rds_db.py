import pymysql
from API.config import credentials as rds

conn = pymysql.connect(
    host=rds.HOST,
    port=rds.PORT,
    user=rds.USER,
    password=rds.PASSWORD,
    db=rds.DATABASE
)

'''TEMPERATURE'''


def insert_temperature(process_id, temperature, timestamp):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Temperatures (process_id, temperature, timestamp) "
                f"VALUES ({process_id}, {temperature}, {timestamp})")
    return conn.commit()


def get_temperature(process_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Temperatures WHERE process_id = {process_id}")
    temperature = cur.fetchone()
    return temperature


'''PROCESS'''


def insert_process(fermenter_id, state, stage, timestamp):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Processes (fermenter_id, state, stage, timestamp) "
                f"VALUES ({fermenter_id}, {state}, {stage}, {timestamp})")
    return conn.commit()


def get_process(process_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Processes WHERE process_id = {process_id}")
    process = cur.fetchone()
    return process


'''BEER'''


def insert_beer(beer_id, description):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Beers (beer_id, description) "
                f"VALUES ({beer_id}, {description})")
    return conn.commit()


def get_beer(beer_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Beers WHERE beer_id = {beer_id}")
    beer = cur.fetchone()
    return beer


'''CARBONATOR'''


def insert_carbonator(carbonator_id, beer_id):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Carbonators (carbonator_id, beer_id) "
                f"VALUES ({carbonator_id}, {beer_id})")
    return conn.commit()


def get_carbonator(carbonator_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Carbonators WHERE carbonator_id = {carbonator_id}")
    carbonator = cur.fetchone()
    return carbonator


'''FERMENTER'''


def insert_fermenter(fermenter_id, beer_id):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Fermenters (fermenter_id, beer_id) "
                f"VALUES ({fermenter_id}, {beer_id})")
    return conn.commit()


def get_fermenter(fermenter_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Fermenters WHERE fermenter_id = {fermenter_id}")
    fermenter = cur.fetchone()
    return fermenter
