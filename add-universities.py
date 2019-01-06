import json
from pprint import pprint
import googlemaps
import config
from datetime import datetime
import time
import sys, traceback

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
    # print('New city: ' + city_name)
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

def update_education_with_details(cur, education_id, name, address, zipcode, homepage, city_id, phone, place_id, url, lat, lng):
    """ Update education existing record in educations table  """
    dt = datetime.now()
    updated_rows = 0
    sql = "UPDATE educations SET name = %s, address = %s, zipcode = %s, homepage = %s, city_id = %s, phone = %s, place_id = %s, url = %s, lat = %s, lng = %s, updated_at = %s WHERE id = %s;"
    try:
        # execute the UPDATE statement
        cur.execute(sql, (name, address, zipcode, homepage, city_id, phone, place_id, url, lat, lng, dt, education_id))
        updated_rows = cur.rowcount
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return updated_rows == 1

def update_education(cur, education_id, name, address, zipcode, city_id, place_id, lat, lng):
    """ Update education existing record in educations table  """
    dt = datetime.now()
    updated_rows = 0
    sql = "UPDATE educations SET name = %s, address = %s, zipcode = %s, city_id = %s, place_id = %s, lat = %s, lng = %s, updated_at = %s WHERE id = %s;"
    try:
        # execute the UPDATE statement
        cur.execute(sql, (name, address, zipcode, city_id, place_id, lat, lng, dt, education_id))
        updated_rows = cur.rowcount
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return updated_rows == 1

def insert_education(cur, name, address, zipcode, city_id, place_id, lat, lng):
    """ Insert education name and city id into educations table  """
    dt = datetime.now()
    sql = "INSERT INTO educations(name, address, zipcode, city_id, place_id, lat, lng, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    try:
        # execute the INSERT statement
        cur.execute(sql, (name, address, zipcode, city_id, place_id, lat, lng, dt, dt))
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return False

def insert_education_with_details(cur, name, address, zipcode, homepage, city_id, phone, place_id, url, lat, lng):
    """ Insert education name and city id into educations table  """
    dt = datetime.now()
    sql = "INSERT INTO educations(name, address, zipcode, homepage, city_id, phone, place_id, url, lat, lng, created_at, updated_at) VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    try:
        # execute the INSERT statement
        cur.execute(sql, (name, address, str(zipcode), homepage, int(city_id), str(phone), str(place_id), str(url), str(lat), str(lng), dt, dt))
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return -1
    return False

def get_education_id(cur, name):
    """ Get education ID from educations table  """
    try:
        if(name):
            cur.execute("SELECT id FROM educations WHERE name = %(str)s", {'str': name })
            row = cur.fetchone()
            if row is None:
                return -1
            return int(row[0])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return -1
    return -1

def get_zipcode_from_components(address_components):
    """ Get zipcode from JSON  """
    name = 0
    for component in address_components:
        for component_type in component['types']:
            if component_type == 'postal_code':
                return component.get('long_name', 0)
    return name

def get_state_name_and_id(address_components, cur):
    """ Get state from JSON, then get state_id from states table  """
    name = 0
    name_id = 0
    for component in address_components:
        for component_type in component['types']:
            if component_type == 'administrative_area_level_1':
                name = component.get('long_name','California')
                # TODO THIS IS NOW BROKEN, FIX IT
                name_id = get_state_id(cur, name)
                return name_id
    return name_id

def get_city_name_and_id(address_components, component, cur):
    """ Get city from JSON, then get city_id from cities table  """
    # city_id = 6012
    # state_id = 53
    for component_type in component['types']:
        if component_type == 'locality':
            short_name = component.get('short_name','Orlando')
            # TODO THIS IS NOW BROKEN, FIX IT
            city_id = get_city_id(cur, short_name)
            if city_id == 0:
                long_name = component.get('long_name','Orlando')
                # TODO THIS IS NOW BROKEN, FIX IT
                city_id = get_city_id(cur, long_name)
                if city_id == 0:
                    print('Inserting: ' + short_name)
                    try:
                        state_id = get_state_name_and_id(address_components, cur)
                        # insert new city and get ID for new city
                        city_id = insert_city(cur, short_name, state_id)
                        return city_id
                    except:
                        return 6012
    return city_id

def handle_address(address_components, cur):
    """ Get city_id from JSON """
    city_id = 0
    for component in address_components:
        if city_id == 0:
            city_id = get_city_name_and_id(address_components, component, cur)
    return city_id

def insert_or_update(conn, cur2, cur3, name, education_id, place_id, loc, results, city_id, zipcode):

    address = results.get('formatted_address','')
    address = address.rstrip('\n')
    website = results.get('website','')
    phone = results.get('formatted_phone_number', '')
    url = results.get('url', '')
    lat = loc.get('lat', '')
    lng = loc.get('lng', '')

    try:
        if education_id == -1:
            # print('Insert education')
            if insert_education_with_details(cur2, name, address, zipcode, website, city_id, phone, place_id, url, lat, lng):
                # Commit the changes to the database
                conn.commit()
                print(name)
            elif insert_education_with_details(cur2, name, '', '', '', city_id, '', place_id,'', '', ''):
                # Commit the changes to the database
                conn.commit()

        else:
            # print('Update education')
            if update_education_with_details(cur3, education_id, name, address, zipcode, website, city_id, phone, place_id, url, lat, lng):
                # Commit the changes to the database
                conn.commit()
                print(name)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        # traceback.print_exc(file=sys.stdout)
        if insert_education_with_details(cur2, name, '', '', '', city_id, '', place_id,'', '', ''):
            print(name)
            # Commit the changes to the database
            conn.commit()

def get_details(conn, cur1, cur2, cur3, gmaps, education_id, place_id, loc,
        place_name, formatted_address):

    try:
        # results = gmaps.place(place_id, 'ChIJN1t_tDeuEmsRUsoyG83frY4', fields=['name', 'rating','formatted_phone_number','address_component', 'formatted_address', 'url', 'website'])
        results = gmaps.place(place_id, fields=['name', 'formatted_phone_number', 'url', 'website'])
        formatted_address = results['result'].get('formatted_address', '')
        zipcode = 0
        city_id = 0
        try:
            if address_components:
                zipcode = get_zipcode_from_components(formatted_address)
                city_id = handle_address(address_components, cur1)
        except:
            print("Unexpected error in zipcode or city_id:", sys.exc_info()[0])

        # print('Zipcode: ' + str(zipcode))
        # print('{} ({}, {})'.format(place_name, loc['lng'], loc['lat']))
        # if zipcode == 0:
        # print(place_name + 'has zipcode 0')
        insert_or_update(conn, cur2, cur3, place_name, education_id, place_id, loc, results['result'], city_id, zipcode)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        traceback.print_exc(file=sys.stdout)
        print(place_name + ' Failed')

def get_zipcode_from_address(address):

    str_list = [x.strip() for x in address.split(',')]
    str_list = list(filter(None, str_list))
    string = str_list[len(str_list)-2]
    list_of_strings = string.split()
    if len(list_of_strings) == 2:
        return list_of_strings[1]
    return -1

def get_city_id_from_address(conn, cur1, cur2, address):

    city_id = -1
    state_id = -1
    str_list = [x.strip() for x in address.split(',')]
    str_list = list(filter(None, str_list))
    country = str_list[len(str_list)-1]
    if country == 'USA' or country == 'United States':
        abbreviation = get_state(str_list[len(str_list)-2])
        city_name = str_list[len(str_list)-3]
        state_id = get_state_id(cur1, abbreviation)

        if state_id != -1:
            city_id = get_city_id(cur1, city_name, state_id)
            if city_id == -1:
                print('INSERT: ' + city_name)
                city_id = insert_city(cur2, city_name, state_id)
                if city_id == -1:
                    print('Error! Cannot insert: ' + city_name)
                else:
                    conn.commit()
        else:
            print('Error! Cannot find state: ' + abbreviation)
    else:
        print('Error! Cannot find country: ' + country)
    return city_id

def search(conn, cur0, cur1, cur2, cur3, name, gmaps, education_id):

    try:
        lat = -1
        lng = -1
        input_type = 'textquery'
        places = gmaps.find_place(name, input_type, fields=['name', 'geometry', 'formatted_address', 'place_id'])
        if places['status'] == 'OK':
            if places['candidates']:
                place = places['candidates'][0]
                place_name = place.get('name', '')
                address = place.get('formatted_address', '')
                address = address.rstrip('\n')
                city_id = get_city_id_from_address(conn, cur0, cur1, address)
                if city_id == -1:
                    print(place_name + ' CITY NOT FOUND')
                    city_id = 6012
                zipcode = get_zipcode_from_address(address)
                place_id = place.get('place_id', '')
                geometry = place.get('geometry', '')
                if geometry:
                    loc = geometry.get('location', '')
                    if loc:
                        lat = loc.get('lat', '')
                        lng = loc.get('lng', '')
                if education_id == -1:
                    # check if Google Place Name exists
                    education_id = get_education_id(cur0, place.get('name', ''))
                    if education_id == -1:
                        return insert_education(cur2, place_name, address, zipcode, city_id, place_id, lat, lng)
                    else:
                        return update_education(cur3, education_id, place_name, address, zipcode, city_id, place_id, lat, lng)
                else:
                    return update_education(cur3, education_id, place_name, address, zipcode, city_id, place_id, lat, lng)

        else:
            print(name + ' NOT FOUND')
    except:
        print("Unexpected error:", sys.exc_info()[0])
        traceback.print_exc(file=sys.stdout)
        print(name + ' Failed')

def setup_search(conn, cursor1, cursor2, cursor3, cursor4, education_id, name, gmaps):
    if education_id == -1:
        education_id = get_education_id(cursor1, name)
    if education_id != -1:
        if search(conn, cursor1, cursor2, cursor3, cursor4, name, gmaps, education_id):
            conn.commit()
            return 1
    else:
        if search(conn, cursor1, cursor2, cursor3, cursor4, name, gmaps, -1):
            conn.commit()
            return 2

    return 0

def parse_file(conn, filename):

    count_insert = 0
    count_update = 0
    count_total = 0
    with open(filename, "r") as f:
        try:
            # load json
            data = json.load(f)
            # Open a cursor to perform database operations
            cursor1 = conn.cursor()
            cursor2 = conn.cursor()
            cursor3 = conn.cursor()
            cursor4 = conn.cursor()
            gmaps = googlemaps.Client(key=config.API_KEY)
            for d in data:
                flag =  setup_search(conn, cursor1, cursor2, cursor3, cursor4, -1, d["institution"], gmaps)

                if flag == 1:
                    count_update = count_update + 1
                elif flag == 2:
                    count_insert = count_insert + 1

                count_total = count_total + 1
                if count_total % 100 == 0:
                    print('Count Update: ' + str(count_update))
                    print('Count Insert: ' + str(count_insert))
                    print('Count Total: ' + str(count_total))
                    print('Last University: ' + d["institution"])
                    # Wait for 5 seconds to avoid rate limiting
                    time.sleep(5)

        finally:
            print('Count Update: ' + str(count_update))
            print('Count Insert: ' + str(count_insert))
            print('Count Total: ' + str(count_total))
            print('Closing cursor1')
            cursor1.close()
            print('Closing cursor2')
            cursor2.close()
            print('Closing cursor3')
            cursor3.close()
            print('Closing cursor4')
            cursor4.close()

def update_schools(conn):
    try:
        # Open a cursor to perform database operations
        cursor1 = conn.cursor()
        cursor2 = conn.cursor()
        cursor3 = conn.cursor()
        cursor4 = conn.cursor()
        count_insert = 0
        count_update = 0
        count_total = 0
        cursor0 = conn.cursor()
        sql = "SELECT id, name FROM educations WHERE zipcode IS NULL AND city_id != 6012 ORDER BY name LIMIT 10000"
        # sql = "SELECT id, address FROM educations WHERE address IS NOT NULL AND lat IS NOT NULL ORDER BY name LIMIT 100000"
        # sql = "SELECT id, address FROM educations WHERE lat IS NOT NULL ORDER BY name LIMIT 100000"
        cursor0.execute(sql)
        offset = 0
        gmaps = googlemaps.Client(key=config.API_KEY)
        while True:
            rows = cursor0.fetchmany(1000)
            offset = offset + 1000
            if not rows:
                break
            for row in rows:
                flag =  setup_search(conn, cursor1, cursor2, cursor3, cursor4, str(row[0]), str(row[1]), gmaps)

                if flag == 1:
                    count_update = count_update + 1
                elif flag == 2:
                    count_insert = count_insert + 1
                count_total = count_total + 1
                if count_total % 100 == 0:
                    print('Count Update: ' + str(count_update))
                    print('Count Insert: ' + str(count_insert))
                    print('Count Total: ' + str(count_total))
                    print('Last University: ' + str(row[1]))
                    # Wait for 5 seconds to avoid rate limiting
                    time.sleep(5)

    finally:
        print('Count Update: ' + str(count_update))
        print('Count Insert: ' + str(count_insert))
        print('Count Total: ' + str(count_total))
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
        update_schools(conn)
        # parse_file(conn, 'us_institutions.json')

    finally:
        print('Closing connection to database')
        # Close communication with the database
        conn.close()

if __name__ == '__main__':
    main()
