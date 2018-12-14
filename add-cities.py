import config
import json
import psycopg2
from datetime import datetime

def database():
    """ Connect to the database """
    # Define our connection string
    conn_string = "host='localhost' dbname='{}' user='{}' password = '{}'".format(
        config.DB_NAME, config.USER, config.PASSWORD)
    # print the connection string we will use to connect
    print("Connecting to database\n ->%s" % (conn_string))

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    print("Connected!\n")
    return conn

def insert_city(cur, city_name, state_id):
    """ Insert city name and state id into cities table  """
    dt = datetime.now()
    # print('City name: ' + city_name)
    # print('State ID: ' + str(state_id))
    sql = "INSERT INTO cities(name, state_id, created_at, updated_at) VALUES (%s, %s, %s, %s)"
    try:
        # execute the INSERT statement
        cur.execute(sql, (city_name, state_id, dt, dt))
        return True
    except:
        return False

def get_state_id(cur, name):
    """ Get state ID from states table  """
    # print(name)
    cur.execute("SELECT id FROM states WHERE name = %(str)s", {'str': name })
    row = cur.fetchone()
    if row is None:
        return -1
    return int(row[0])

def read_file(conn, filename):
    """ Read city name and state name from json file """

    with open(filename, "r") as f:
        try:
            # Open a cursor to perform database operations
            cursor1 = conn.cursor()
            cursor2 = conn.cursor()
            # load json
            data = json.load(f)
            for d in data:
                state_id = get_state_id(cursor1, d['state'])
                if state_id != -1:
                    if insert_city(cursor2, d['city'], state_id):
                        # Commit the changes to the database
                        conn.commit()
                    else:
                        print('Error: Insert City ' + d['city'])

                else:
                    print('Error: get_state_id ' + d['city'] + d['state'])

        finally:
            print('Closing cursor1')
            cursor1.close()
            print('Closing cursor2')
            cursor2.close()

def main():
    try:
        # Open a connection to the database
        conn = database()
        read_file(conn, 'usaCities.json')

    finally:
        print('Closing connection to database')
        # Close communication with the database
        conn.close()

if __name__ == '__main__':
    main()
