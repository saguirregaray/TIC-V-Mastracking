from resources.config import credentials as rds
from pymysqlpool.pool import Pool
import csv
from io import StringIO
from flask import make_response


pool = Pool(host=rds.HOST,
            port=rds.PORT,
            user=rds.USER,
            password=rds.PASSWORD,
            db=rds.DATABASE)
pool.init()

'''PROCESS'''


def insert_process(fecha_inicio, fecha_fin, stage, state, fermenter_id, beer_id, name):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Processes (fecha_inicio, fecha_finalizacion, stage, state, fermenter_id, "
                f"beer_id, name) VALUES ('{fecha_inicio}', '{fecha_fin}', '{stage}', '{state}'"
                f", {fermenter_id}, {beer_id}, '{name}')")
    conn.commit()
    pool.release(conn)
    return get_process(cur.lastrowid)


def get_process(process_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Processes WHERE id = {process_id} AND deleted = false")
    process = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return process


def get_processes():
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Processes WHERE deleted = false")
    processes = cur.fetchall()
    pool.release(conn)
    return processes


def get_active_processes():
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute('''
        SELECT p.deleted, p.id, p.fecha_inicio, p.stage, t.temperature as current_temperature,
         f.name as fermenter, f.id as fermenter_id, f.physical_id as
         fermenter_physical_id, c.name as carbonator, c.id as carbonator_id, c.physical_id as
         carbonator_physical_id, b.name as beer, b.id as beer_id,
          b.maduration_temp as maduration_temp, b.fermentation_temp as fermentation_temp,
          t.target_temperature as target_temperature, t.id as temp_id, p.alarm_activated, 
          p.alarm_deactivation_timestamp, p.alarm_hours_deactivated, t.timestamp, p.name
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
    pool.release(conn)
    return processes


def modify_process_stage(process_id, current_stage, target_stage, machine_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    if current_stage == 'fermentation' and target_stage == "maduration":
        if machine_id is None:
            cur.execute(f"UPDATE Processes SET stage = '{target_stage}', state = 1 WHERE id = {process_id}")
        else:
            cur.execute(f"UPDATE Processes SET stage = '{target_stage}', fermenter_id = {machine_id},"
                        f" state = 1 WHERE id = {process_id}")
    elif current_stage == 'fermentation' and target_stage == "carbonation":
        cur.execute(f"UPDATE Processes SET stage = '{target_stage}', carbonator_id = {machine_id},"
                    f" state = 1 WHERE id = {process_id}")
    elif current_stage == 'maduration' and target_stage == "fermentation":
        if machine_id is None:
            cur.execute(f"UPDATE Processes SET stage = '{target_stage}', state = 1 WHERE id = {process_id}")
        else:
            cur.execute(f"UPDATE Processes SET stage = '{target_stage}', fermenter_id = {machine_id},"
                        f" state = 1 WHERE id = {process_id}")
    elif current_stage == 'maduration' and target_stage == "carbonation":
        cur.execute(f"UPDATE Processes SET stage = '{target_stage}', carbonator_id = {machine_id},"
                    f" state = 1 WHERE id = {process_id}")
    elif current_stage == 'carbonation' and (target_stage == "fermentation" or target_stage == "maduration"):
        cur.execute(f"UPDATE Processes SET stage = '{target_stage}', fermenter_id = {machine_id},"
                    f" state = 1 WHERE id = {process_id}")
    elif target_stage == "end":
        cur.execute(f"UPDATE Processes SET stage = '{target_stage}', state = 0 WHERE id = {process_id}")
    conn.commit()
    pool.release(conn)
    return get_process(process_id)


def get_active_processes_csv():
    si = StringIO()
    cw = csv.writer(si)
    conn = pool.get_conn()
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
    rows = cur.fetchall()
    conn.commit()
    pool.release(conn)
    cw.writerow([i[0] for i in cur.description])
    for row in rows:
        cw.writerow(row.values())
    response = make_response(si.getvalue())
    response.headers['Content-Disposition'] = 'attachment; filename=report.csv'
    response.headers["Content-type"] = "text/csv"
    return response


def get_process_temperature_csv(process_id):
    si = StringIO()
    cw = csv.writer(si)
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f'''
        SELECT  p.id,
                p.fecha_inicio,
                p.fecha_finalizacion,
                p.stage,
                p.state,
                p.fermenter_id,
                p.carbonator_id,
                p.beer_id,
                p.alarm_activated,
                p.name,
                t.timestamp,
                t.temperature,
                t.target_temperature
        FROM Processes p 
        LEFT JOIN Temperatures t ON t.process_id = p.id
        WHERE p.deleted = false and p.id = {process_id}
    ''')
    rows = cur.fetchall()
    conn.commit()
    pool.release(conn)

    cw.writerow([i[0] for i in cur.description])
    for row in rows:
        cw.writerow(row.values())
    response = make_response(si.getvalue())
    response.headers['Content-Disposition'] = 'attachment; filename=report.csv'
    response.headers["Content-type"] = "text/csv"
    return response


'''BEER'''


def insert_beer(name, maduration_temp, fermentation_temp):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Beers (name, maduration_temp, fermentation_temp) "
                f"VALUES ('{name}', {maduration_temp}, {fermentation_temp})")
    conn.commit()
    pool.release(conn)
    return get_beer(cur.lastrowid)


def get_beer(beer_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Beers WHERE id = {beer_id} AND deleted = false")
    beer = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return beer


def delete_beer(beer_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE Beers SET deleted = {True} WHERE id = {beer_id}")
    beer = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return beer


def get_beers():
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Beers WHERE deleted = false")
    beer = cur.fetchall()
    conn.commit()
    pool.release(conn)
    return beer


'''CARBONATOR'''


def insert_carbonator(name, physical_id):
    if get_carbonator_by_physical(physical_id) is None:
        conn = pool.get_conn()
        cur = conn.cursor()
        cur.execute(f"INSERT INTO Carbonators (name, physical_id) "
                    f"VALUES ('{name}', {physical_id})")
        conn.commit()
        pool.release(conn)
        return get_carbonator(cur.lastrowid), 200
    else:
        return f"There is already a Carbonator with the physical id {physical_id}", 409


def get_carbonator(carbonator_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Carbonators WHERE id = {carbonator_id} AND deleted = false")
    carbonator = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return carbonator


def get_carbonator_by_physical(physical_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Carbonators WHERE physical_id = {physical_id} AND deleted = false")
    carbonator = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return carbonator


def get_carbonators():
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Carbonators WHERE deleted = false")
    carbonators = cur.fetchall()
    conn.commit()
    pool.release(conn)
    return carbonators


def delete_carbonator(carbonator_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE Carbonators SET deleted = {True} WHERE id = {carbonator_id}")
    carbonator = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return carbonator


def get_free_carbonators():
    conn = pool.get_conn()
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
    pool.release(conn)
    return carbonators


'''FERMENTER'''


def insert_fermenter(name, physical_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    if get_fermenter_by_physical(physical_id) is None:
        cur.execute(f"INSERT INTO Fermenters (name, physical_id) "
                    f"VALUES ('{name}', {physical_id})")
        conn.commit()
        pool.release(conn)
        return get_fermenter(cur.lastrowid), 200
    else:
        return f"There is already a Fermenter with the physical id {physical_id}", 409


def get_fermenter(fermenter_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Fermenters WHERE id = {fermenter_id} AND deleted = false")
    fermenter = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return fermenter


def get_fermenter_by_physical(physical_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Fermenters WHERE physical_id = {physical_id} AND deleted = false")
    fermenter = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return fermenter


def get_fermenters():
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Fermenters WHERE deleted = false")
    fermenters = cur.fetchall()
    conn.commit()
    pool.release(conn)
    return fermenters


def delete_fermenter(fermenter_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE Fermenters SET deleted = {True} WHERE id = {fermenter_id}")
    fermenter = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return fermenter


def get_free_fermenters():
    conn = pool.get_conn()
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
    pool.release(conn)
    return fermenters


'''TEMPERATURE'''


def insert_temperature(temperature, timestamp, process_id, target_temperature):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f'''INSERT INTO Temperatures (timestamp, temperature, process_id, target_temperature) 
                  VALUES ("{timestamp}", {temperature}, {process_id}, {target_temperature})''')
    conn.commit()
    pool.release(conn)
    return get_temperature(cur.lastrowid)


def get_temperature(temp_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Temperatures WHERE id = {temp_id}  AND deleted = false")
    temperature = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return temperature


def modify_target_temp(temp_id, target_temperature):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE Temperatures SET target_temperature = {target_temperature} WHERE id = {temp_id}")
    conn.commit()
    pool.release(conn)
    return get_temperature(temp_id)


def get_temperature_by_process(process_id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Temperatures "
                f"WHERE process_id = {process_id} "
                f"AND (timestamp in (SELECT max(timestamp) FROM Temperatures GROUP BY process_id) OR timestamp is null)"
                f" AND deleted = false")
    temperature = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return temperature


'''ALERTS'''


def insert_alert(process_id, description, stage, timestamp):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"INSERT INTO Alerts (process_id, description, stage, alert_timestamp) "
                f"VALUES ('{process_id}', '{description}', '{stage}', '{timestamp}')")
    conn.commit()
    pool.release(conn)
    return get_alert(cur.lastrowid)


def get_alert(id):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT *  FROM Alerts WHERE id = {id} AND deleted = false")
    alert = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return alert


def get_alerts():
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Alerts WHERE deleted = false")
    alerts = cur.fetchall()
    conn.commit()
    pool.release(conn)
    return alerts


def get_alerts_csv():
    si = StringIO()
    cw = csv.writer(si)
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM Alerts WHERE deleted = false")
    rows = cur.fetchall()
    conn.commit()
    pool.release(conn)

    cw.writerow([i[0] for i in cur.description])
    for row in rows:
        cw.writerow(row.values())
    response = make_response(si.getvalue())
    response.headers['Content-Disposition'] = 'attachment; filename=report.csv'
    response.headers["Content-type"] = "text/csv"
    return response


def deactivate_alert(process_id, alarm_deactivation_timestamp, alarm_hours_deactivated, alarm_activated):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE Processes SET alarm_activated = {alarm_activated},  "
                f"alarm_deactivation_timestamp = '{alarm_deactivation_timestamp}', "
                f"alarm_hours_deactivated = {alarm_hours_deactivated} "
                f"WHERE id = {process_id}")
    conn.commit()
    pool.release(conn)
    return get_process(process_id)


def activate_alert(process_id, alarm_activated):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE Processes SET alarm_activated = {alarm_activated}  "
                f"WHERE id = {process_id}")
    conn.commit()
    pool.release(conn)
    return get_process(process_id)


'''MAILS'''


def insert_mail(mail_address):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f'''INSERT INTO Mails (mail_address) VALUES ("{mail_address}")''')
    conn.commit()
    pool.release(conn)
    return get_mail(mail_address)


def get_mails():
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT mail_address FROM Mails")
    mails = cur.fetchall()
    conn.commit()
    pool.release(conn)
    return mails


def get_mail(mail_address):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT mail_address FROM Mails WHERE mail_address = '{mail_address}'")
    mails = cur.fetchall()
    conn.commit()
    pool.release(conn)
    return mails


def delete_mail(mail_address):
    conn = pool.get_conn()
    cur = conn.cursor()
    cur.execute(f'''DELETE FROM Mails WHERE mail_address = "{mail_address}"''')
    mail = cur.fetchone()
    conn.commit()
    pool.release(conn)
    return mail

