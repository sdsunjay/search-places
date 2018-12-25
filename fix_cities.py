# you may also want to remove whitespace characters like `\n` at the end of each line
import config
from datetime import datetime

def insert_city(cur, city_name, state_id):
    """ Insert city name, state id into cities table and get ID of new record """
    dt = datetime.now()
    print('New city name: ' + city_name)
    # print('State ID: ' + str(state_id))
    sql = "INSERT INTO cities(name,\
            state_id, created_at,\
            updated_at) VALUES (%s,\
            %s, %s, %s) RETURNING id"
    try:
        # execute the INSERT statement
        cur.execute(sql, (city_name, state_id, dt, dt))
        return cur.fetchone()[0]
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return -1

def get_state_id(cur, name):
    """ Get state ID from states table  """
    # print('State: ' + str(name))
    cur.execute("SELECT id FROM states WHERE iso = %(str)s", {'str': name })
    row = cur.fetchone()
    if row is None:
        return -1
    return int(row[0])

def get_city_id(cur, name, state_id):
    """ Get city ID from city table  """
    # print(name)
    cur.execute("SELECT id FROM cities WHERE name = %(str)s AND state_id = %(state_id)s", {'str': name, 'state_id': state_id })
    row = cur.fetchone()
    if row is None:
        return -1
    return int(row[0])

def get_state(state_and_zip):
    """ Get state abbreviation  """
    return state_and_zip[:2]

def help_read_file(conn, lines, cursor1, cursor2, cursor3):

    for line in lines:
        str_list = [x.strip() for x in line.split(',')]
        str_list = list(filter(None, str_list))
        country = str_list[len(str_list)-1]
        if country == 'USA':
            abbreviation = get_state(str_list[len(str_list)-2])
            city_name = str_list[len(str_list)-3]
            # print(city_name + ', ' + abbreviation)
            state_id = get_state_id(cursor1, abbreviation)
            if state_id != -1:
                city_id = get_city_id(cursor2, city_name, state_id)
                if city_id == -1:
                    city_id = insert_city(cursor3, city_name, state_id)
                    if city_id == -1:
                        print('Error! Cannot insert: ' + city_name)
                        # print('New city not inserted!')
                    conn.commit()
                print(str(city_id))
            else:
                print('Error! Cannot find state: ' + abbreviation)
        else:
            print('Error! Cannot find country: ' + country)

def read_file(conn, filename):

    try:
        print('Opening cursor1')
        cursor1 = conn.cursor()
        print('Opening cursor2')
        cursor2 = conn.cursor()
        print('Opening cursor3')
        cursor3 = conn.cursor()
        lines = [line.rstrip('\n') for line in open(filename)]
        help_read_file(conn, lines, cursor1, cursor2, cursor3)

    finally:
        print('Closing cursor1')
        cursor1.close()
        print('Closing cursor2')
        cursor2.close()
        print('Closing cursor3')
        cursor3.close()

def main():
    try:
        # Open a connection to the database
        conn = config.database()
        read_file(conn, 'address.txt')
    finally:
        print('Closing connection to database')
        # Close communication with the database
        conn.close()

if __name__ == '__main__':
    main()
