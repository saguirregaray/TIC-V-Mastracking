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


def insert_process(fecha_inicio, fecha_fin, stage, state, fermenter_id, beer_id):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Processes (fecha_inicio, fecha_finalizacion, stage, state, fermenter_id, "
                f"beer_id) VALUES ('{fecha_inicio}', '{fecha_fin}', '{stage}', '{state}'"
                f", {fermenter_id}, {beer_id})")
    conn.commit()
    return get_process(cur.lastrowid)


def get_process(process_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Processes WHERE id = {process_id} AND deleted = false")
    process = cur.fetchone()
    return process


def get_processes():
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Processes WHERE deleted = false")
    processes = cur.fetchall()
    return processes


def get_active_processes():
    cur = conn.cursor()
    cur.execute('''
        SELECT p.deleted, p.id, p.fecha_inicio, p.stage, t.temperature as current_temperature,
         f.name as fermenter, c.name as carbonator, b.name as beer, b.id as beer_id,
          b.maduration_temp as maduration_temp, b.fermentation_temp as fermentation_temp,
          t.target_temperature as target_temperature, t.id as temp_id
        FROM Processes p 
        LEFT JOIN Temperatures t ON t.process_id = p.id
        JOIN Fermenters f ON f.id = p.fermenter_id 
        LEFT JOIN Carbonators c ON p.carbonator_id = c.id
        JOIN Beers b ON p.beer_id = b.id 
        WHERE p.state = 1 
            and (t.`timestamp` in (SELECT max(t.`timestamp`)
                                    FROM Processes p JOIN Temperatures t ON t.process_id = p.id 
                                    GROUP BY p.id)
            or t.`timestamp` is null)
            and p.deleted = false
    ''')
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
    cur.execute(f"SELECT *  FROM Beers WHERE id = {beer_id} AND deleted = false")
    beer = cur.fetchone()
    return beer


def delete_beer(beer_id):
    cur = conn.cursor()
    cur.execute(f"UPDATE Beers SET deleted = {True} WHERE id = {beer_id}")
    beer = cur.fetchone()
    return beer


def get_beers():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Beers WHERE deleted = false")
    beer = cur.fetchall()
    return beer


'''CARBONATOR'''


def insert_carbonator(name, physical_id):
    if get_carbonator_by_physical(physical_id) is None:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO Carbonators (name, physical_id) "
                    f"VALUES ('{name}', {physical_id})")
        conn.commit()
        return get_carbonator(cur.lastrowid)
    else:
        return f"There is already a Carbonator with the physical id {physical_id}", 500


def get_carbonator(carbonator_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Carbonators WHERE id = {carbonator_id} AND deleted = false")
    carbonator = cur.fetchone()
    return carbonator


def get_carbonator_by_physical(physical_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Carbonators WHERE physical_id = {physical_id} AND deleted = false")
    carbonator = cur.fetchone()
    return carbonator


def get_carbonators():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Carbonators WHERE deleted = false")
    carbonators = cur.fetchall()
    return carbonators


def delete_carbonator(carbonator_id):
    cur = conn.cursor()
    cur.execute(f"UPDATE Carbonators SET deleted = {True} WHERE id = {carbonator_id}")
    carbonator = cur.fetchone()
    return carbonator


def get_free_carbonators():
    cur = conn.cursor()
    cur.execute(f"SELECT Carbonators.id, Carbonators.name FROM Carbonators "
                f"LEFT JOIN Processes "
                f"ON Carbonators.id = Processes.carbonator_id "
                f"WHERE Processes.carbonator_id IS NULL"
                f"AND deleted = false")
    carbonators = cur.fetchall()
    return carbonators


'''FERMENTER'''


def insert_fermenter(name, physical_id):
    cur = conn.cursor()
    if get_fermenter_by_physical(physical_id) is None:
        cur.execute(f"INSERT INTO Fermenters (name, physical_id) "
                    f"VALUES ('{name}', {physical_id})")
        conn.commit()
        return get_fermenter(cur.lastrowid)
    else:
        return f"There is already a Fermenter with the physical id {physical_id}", 500


def get_fermenter(fermenter_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Fermenters WHERE id = {fermenter_id} AND deleted = false")
    fermenter = cur.fetchone()
    return fermenter


def get_fermenter_by_physical(physical_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Fermenters WHERE physical_id = {physical_id} AND deleted = false")
    fermenter = cur.fetchone()
    return fermenter


def get_fermenters():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Fermenters WHERE deleted = false")
    fermenters = cur.fetchall()
    return fermenters


def delete_fermenter(fermenter_id):
    cur = conn.cursor()
    cur.execute(f"UPDATE Fermenters SET deleted = {True} WHERE id = {fermenter_id}")
    fermenter = cur.fetchone()
    conn.commit()
    return fermenter


def get_free_fermenters():
    cur = conn.cursor()
    cur.execute(f"SELECT Fermenters.id, Fermenters.name FROM Fermenters "
                f"LEFT JOIN Processes "
                f"ON Fermenters.id = Processes.fermenter_id "
                f"WHERE Processes.fermenter_id IS NULL"
                f" AND deleted = false")
    fermenters = cur.fetchall()
    return fermenters


'''TEMPERATURE'''


def insert_temperature(temperature, timestamp, process_id, target_temperature):
    cur = conn.cursor()
    cur.execute(f'''INSERT INTO Temperatures (timestamp, temperature, process_id, target_temperature) 
                  VALUES ("{timestamp}", {temperature}, {process_id}, {target_temperature})''')
    conn.commit()
    return get_temperature(cur.lastrowid)


def get_temperature(temp_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Temperatures WHERE id = {temp_id}  AND deleted = false")
    temperature = cur.fetchone()
    return temperature


def modify_target_temp(temp_id, target_temperature):
    cur = conn.cursor()
    cur.execute(f"UPDATE Temperatures SET target_temperature = {target_temperature} WHERE id = {temp_id}")
    conn.commit()
    return get_temperature(temp_id)


'''ALERTS'''


def insert_alert(process_id, description, stage, timestamp):
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Alerts (process_id, description, stage, alert_timestamp) "
                f"VALUES ('{process_id}', '{description}', '{stage}', '{timestamp}')")
    conn.commit()
    return get_alert(cur.lastrowid)


def get_alert(id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Alerts WHERE id = {id}  AND deleted = false")
    alert = cur.fetchone()
    return alert


def get_alerts():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Alerts WHERE deleted = false")
    alerts = cur.fetchall()
    return alerts
