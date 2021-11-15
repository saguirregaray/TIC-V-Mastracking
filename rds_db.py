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
    conn.commit()
    return process


def get_processes():
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Processes WHERE deleted = false")
    processes = cur.fetchall()
    conn.commit()
    return processes


def get_active_processes():
    cur = conn.cursor()
    cur.execute('''
        SELECT p.deleted, p.id, p.fecha_inicio, p.stage, t.temperature as current_temperature,
         f.name as fermenter, c.name as carbonator, b.name as beer, b.id as beer_id,
          b.maduration_temp as maduration_temp, b.fermentation_temp as fermentation_temp,
          t.target_temperature as target_temperature, t.id as temp_id, p.alarm_activated, 
          p.alarm_deactivation_timestamp, p.alarm_hours_deactivated, t.timestamp
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
    conn.commit()
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
    conn.commit()
    return beer


def delete_beer(beer_id):
    cur = conn.cursor()
    cur.execute(f"UPDATE Beers SET deleted = {True} WHERE id = {beer_id}")
    beer = cur.fetchone()
    conn.commit()
    return beer


def get_beers():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Beers WHERE deleted = false")
    beer = cur.fetchall()
    conn.commit()
    return beer


'''CARBONATOR'''


def insert_carbonator(name, physical_id):
    if get_carbonator_by_physical(physical_id) is None:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO Carbonators (name, physical_id) "
                    f"VALUES ('{name}', {physical_id})")
        conn.commit()
        return get_carbonator(cur.lastrowid), 200
    else:
        return f"There is already a Carbonator with the physical id {physical_id}", 409


def get_carbonator(carbonator_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Carbonators WHERE id = {carbonator_id} AND deleted = false")
    carbonator = cur.fetchone()
    conn.commit()
    return carbonator


def get_carbonator_by_physical(physical_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Carbonators WHERE physical_id = {physical_id} AND deleted = false")
    carbonator = cur.fetchone()
    conn.commit()
    return carbonator


def get_carbonators():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Carbonators WHERE deleted = false")
    carbonators = cur.fetchall()
    conn.commit()
    return carbonators


def delete_carbonator(carbonator_id):
    cur = conn.cursor()
    cur.execute(f"UPDATE Carbonators SET deleted = {True} WHERE id = {carbonator_id}")
    carbonator = cur.fetchone()
    conn.commit()
    return carbonator


def get_free_carbonators():
    cur = conn.cursor()
    cur.execute('''
            SELECT c.id, c.name 
            FROM Carbonators c
            WHERE c.id NOT IN (
                SELECT c2.id 
                FROM Processes p
                JOIN Carbonators c2 ON c2.id = p.carbonator_id 
                WHERE p.state = 1 and p.deleted = false)
        ''')
    carbonators = cur.fetchall()
    conn.commit()
    return carbonators


'''FERMENTER'''


def insert_fermenter(name, physical_id):
    cur = conn.cursor()
    if get_fermenter_by_physical(physical_id) is None:
        cur.execute(f"INSERT INTO Fermenters (name, physical_id) "
                    f"VALUES ('{name}', {physical_id})")
        conn.commit()
        return get_fermenter(cur.lastrowid), 200
    else:
        return f"There is already a Fermenter with the physical id {physical_id}", 409


def get_fermenter(fermenter_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Fermenters WHERE id = {fermenter_id} AND deleted = false")
    fermenter = cur.fetchone()
    conn.commit()
    return fermenter


def get_fermenter_by_physical(physical_id):
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Fermenters WHERE physical_id = {physical_id} AND deleted = false")
    fermenter = cur.fetchone()
    conn.commit()
    return fermenter


def get_fermenters():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Fermenters WHERE deleted = false")
    fermenters = cur.fetchall()
    conn.commit()
    return fermenters


def delete_fermenter(fermenter_id):
    cur = conn.cursor()
    cur.execute(f"UPDATE Fermenters SET deleted = {True} WHERE id = {fermenter_id}")
    fermenter = cur.fetchone()
    conn.commit()
    return fermenter


def get_free_fermenters():
    cur = conn.cursor()
    cur.execute('''
            SELECT f.id, f.name 
            FROM Fermenters f
            WHERE f.id NOT IN (
                SELECT f2.id 
                FROM Processes p
                JOIN Fermenters f2 ON f2.id = p.fermenter_id 
                WHERE p.state = 1 and p.deleted = false)
        ''')
    conn.commit()
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
    conn.commit()
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
    cur.execute(f"SELECT *  FROM Alerts WHERE id = {id} AND deleted = false")
    alert = cur.fetchone()
    conn.commit()
    return alert


def get_alerts():
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Alerts WHERE deleted = false")
    alerts = cur.fetchall()
    conn.commit()
    return alerts


def deactivate_alert(process_id, alarm_deactivation_timestamp, alarm_hours_deactivated, alarm_activated):
    cur = conn.cursor()
    cur.execute(f"UPDATE Processes SET alarm_activated = {alarm_activated},  "
                f"alarm_deactivation_timestamp = '{alarm_deactivation_timestamp}', "
                f"alarm_hours_deactivated = {alarm_hours_deactivated} "
                f"WHERE id = {process_id}")
    conn.commit()
    return get_process(process_id)


def activate_alert(process_id, alarm_activated):
    cur = conn.cursor()
    cur.execute(f"UPDATE Processes SET alarm_activated = {alarm_activated}  "
                f"WHERE id = {process_id}")
    conn.commit()
    return get_process(process_id)
