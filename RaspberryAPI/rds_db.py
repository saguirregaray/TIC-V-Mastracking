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
