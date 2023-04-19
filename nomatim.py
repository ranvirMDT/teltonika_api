import requests
import json
import mysql.connector
import time

# NOTE: Script requires table with the columns that have the same name as JSON data key but with "_" instead of "." 

def reverse_geocode(lat, lon):
    url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=jsonv2"
    response = requests.get(url)
    if response.status_code ==200:
        data = response.json()
        # return data
        if 'road' in data['address'] and 'district' in data['address']:
            return data['address']['road'] + ", " + data['address']['district'] + ", " + data['address']['state']
        elif 'quarter' in data['address'] and 'district' in data['address']:
            return data['address']['quarter'] + ", " + data['address']['district'] + ", " + data['address']['state']
        else:
            return None
    else:
    	return
    
try:
    # Connect to MySQL database
    conn = mysql.connector.connect(user='admin', password='mdti1234', host='emirates-db.cmfexxmoaqnh.ap-southeast-1.rds.amazonaws.com', port= 22235, database='gpsTrack')

    # Get cursor to perform operations on database
    cursor = conn.cursor()

    query = "SELECT id, position_latitude, position_longitude FROM tracker_logs WHERE address IS NULL AND position_latitude IS NOT NULL AND position_longitude IS NOT NULL;"
    cursor.execute(query)
    raw_rows = cursor.fetchall()
    rows = [
        {"id": row[0], "latitude": float(row[1]), "longitude": float(row[2])} for row in raw_rows
    ]
    # print(rows)

    for row in rows:
        id, lat, lon = row["id"], row["latitude"], row["longitude"]
        address = reverse_geocode(lat, lon)
        print(address)

        try:
            if address:
                update_query = "UPDATE tracker_logs SET address = %s WHERE id = %s;"
                cursor.execute(update_query, (address, id))
            else:
                update_query = "UPDATE tracker_logs SET address = 'Address not found' WHERE id = %s;"
                cursor.execute(update_query, (id,))

            # check for missing data
            if cursor.rowcount == 0:
                print("Failed to insert address into tracker_logs, missing data")
            else:
                # Commit changes to database
                conn.commit()
                print("Address successfully inserted!")
        except:
            print('query failed')
        
        time.sleep(1)
        

except mysql.connector.Error as error:
    print("Failed to insert record into teltonikas table {}".format(error))
# Close cursor and connection to database
finally:
    if (conn.is_connected()):
        cursor.close()
        conn.close()
        print("MySQL connection is closed")