import config
from datetime import datetime

def get_state_id(cur, name):
    """ Get state ID from states table  """
    try:
        if name:
            cur.execute("SELECT id FROM states WHERE iso = %(str)s", {'str': name })
            row = cur.fetchone()
            if row is None:
                return -1
            return int(row[0])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return -1
    return -1

def get_state(state_and_zip):
    """ Get state abbreviation  """
    return state_and_zip[:2]

def get_city_id(cur, name, state_id):
    """ Get city ID from city table  """
    try:
        if name:
            cur.execute("SELECT id FROM cities WHERE name = %(str)s AND state_id = %(state_id)s", {'str': name, 'state_id': state_id })
            row = cur.fetchone()
            if row is None:
                return -1
            return int(row[0])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return -1
    return -1

def insert_city(cur, city_name, state_id):
    """ Insert city name, state id into cities table and get ID of new record """
    dt = datetime.now()
    print('New city: ' + city_name)
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

def update_education_zipcode(cur, education_id):
    """ Update education zipcode existing record in educations table  """
    dt = datetime.now()
    updated_rows = 0
    sql = "UPDATE educations SET zipcode = NULL, updated_at = %s WHERE id = %s;"
    # execute the UPDATE statement
    cur.execute(sql, (str(dt), str(education_id)))
    updated_rows = cur.rowcount
    return updated_rows == 1

def update_education(cur, education_id, city_id):
    """ Update education existing record in educations table  """
    dt = datetime.now()
    updated_rows = 0
    sql = "UPDATE educations SET city_id = %s, updated_at = %s WHERE id = %s;"
    # execute the UPDATE statement
    cur.execute(sql, (str(city_id), str(dt), str(education_id)))
    updated_rows = cur.rowcount
    return updated_rows == 1

def get_zipcode(string):
    list_of_strings = string.split()
    if len(list_of_strings) == 2:
        return list_of_strings[1]
    return -1

def parse_address(conn, cursor1, cursor2, cursor3, cursor4, address, row_id):

    city_id = -1
    state_id = -1
    str_list = [x.strip() for x in address.split(',')]
    str_list = list(filter(None, str_list))
    country = str_list[len(str_list)-1]
    if country == 'USA' or country == 'United States':
        abbreviation = get_state(str_list[len(str_list)-2])
        city_name = str_list[len(str_list)-3]
        state_id = get_state_id(cursor1, abbreviation)
        zipcode = get_zipcode(str_list[len(str_list)-2])
        if zipcode == -1:
            update_education_zipcode(cursor4, row_id)
            print('SET ZIPCODE TO NULL, EDUCATION ID: ' + str(row_id))
        if state_id != -1:
            city_id = get_city_id(cursor2, city_name, state_id)
            if city_id == -1:
                # print('INSERT: ' + city_name)
                city_id = insert_city(cursor3, city_name, state_id)
                if city_id == -1:
                    print('Error! Cannot insert: ' + city_name)
                else:
                    conn.commit()
        else:
            print('Error! Cannot find state: ' + abbreviation)
    else:
        print('Error! Cannot find country: ' + country)
    return city_id

def read_file(conn, filename):
    try:
        print('Opening cursor1')
        cursor1 = conn.cursor()
        print('Opening cursor2')
        cursor2 = conn.cursor()
        print('Opening cursor3')
        cursor3 = conn.cursor()
        print('Opening cursor4')
        cursor4 = conn.cursor()
        lines = [line.rstrip('\n') for line in open(filename)]
        for line in lines:
            row_id = -1
            parse_address(conn, cursor1, cursor2, cursor3, cursor4, line, row_id)

    finally:
        print('Closing cursor1')
        cursor1.close()
        print('Closing cursor2')
        cursor2.close()
        print('Closing cursor3')
        cursor3.close()
        print('Closing cursor4')
        cursor4.close()

def help_get_educations(conn, cursor1, cursor2, cursor3, cursor4, row_id, address):

        if address:
            city_id = parse_address(conn, cursor1, cursor2, cursor3, cursor4, address, row_id)
            if city_id == -1:
                print('parse_address failed, ID: ' + str(row_id) )
                return False
            else:
                if update_education(cursor4, row_id, city_id) == False:
                    print('update_education failed, ID: ' + str(row_id) )
                    return False
                else:
                    conn.commit()
                    return True
        else:
            print('Address is empty, ID: ' + str(row_id) )
            return False

def get_educations(conn):
    try:
        offset = 0
        print('Opening cursor0')
        cursor0 = conn.cursor()
        print('Opening cursor1')
        cursor1 = conn.cursor()
        print('Opening cursor2')
        cursor2 = conn.cursor()
        print('Opening cursor3')
        cursor3 = conn.cursor()
        print('Opening cursor4')
        cursor4 = conn.cursor()

        sql = "SELECT id, address FROM educations ORDER BY name LIMIT 10000"
        # sql = "SELECT id, address FROM educations WHERE address IS NOT NULL AND lat IS NOT NULL ORDER BY name LIMIT 100000"
        # sql = "SELECT id, address FROM educations WHERE lat IS NOT NULL ORDER BY name LIMIT 100000"
        cursor0.execute(sql)
        while True:
            rows = cursor0.fetchmany(1000)
            offset = offset + 1000
            if not rows:
                break
            for row in rows:
                help_get_educations(conn, cursor1, cursor2, cursor3, cursor4, str(row[0]), str(row[1]))
    finally:
        print('Offset: ' + str(offset))
        print('Closing cursor0')
        cursor0.close()
        print('Closing cursor1')
        cursor1.close()
        print('Closing cursor2')
        cursor2.close()
        print('Closing cursor3')
        cursor3.close()
        print('Closing cursor4')
        cursor4.close()


def main():
    try:
        # Open a connection to the database
        conn = config.database()
        get_educations(conn)
        #lines = read_file(conn, 'address.txt')
    finally:
        print('Closing connection to database')
        # Close communication with the database
        conn.close()

if __name__ == '__main__':
    main()
