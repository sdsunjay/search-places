import config
import json
import psycopg2
from datetime import datetime

def database():
    """ connect to the database """
    # Define our connection string
    conn_string = "host='localhost' dbname='{}' user='{}' password = '{}'".format(
        config.DB_NAME, config.USER, config.PASSWORD)
    # print the connection string we will use to connect
    print("Connecting to database\n ->%s" % (conn_string))

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    # cursor = conn.cursor()
    print("Connected!\n")
    return conn

def insert_city(cur, city_name, state_id):

    dt = datetime.now()
    sql = "INSERT INTO cities(name, state_id, created_at, updated_at) VALUES (%s, %s, %s, %s)"
    # execute the INSERT statement
    try:
        cur.execute(sql, (city_name, state_id, dt, dt))
        return True
    except:
        return False

def get_state_id(cur, name):
    """ Get state ID from states table  """
    sql = 'SELECT id FROM states WHERE name LIKE %s'
    cur.execute(sql, (name,))
    row = cur.fetchone()
    if row is None:
        return -1
    return row[0]

def read_file(conn, filename):

    with open(filename, "r") as f:
        data = json.load(f)
        # Open a cursor to perform database operations
        cursor = conn.cursor()
        for d in data:
            state_id = get_state_id(cursor, d['state'])
            if state_id != -1:
                if insert_city(cursor, d['city'], state_id):
                    # Commit the changes to the database
                    conn.commit()
                else:
                    print('Error: Insert City ' + d['city'])

            else:
                print('Error: get_state_id ' + d['city'] + d['state'])

        cursor.close()

def main():
    try:
        # Open a connection to the database
        conn = database()
        read_file(conn, 'small_usaCities.js')

    finally:
        print('Closing connection to database')
        # Close communication with the database
        conn.close()

if __name__ == '__main__':
    main()
