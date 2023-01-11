import requests
import json
import mysql.connector
import time


try:
    # API url
    url = "https://flespi.io/gw/channels/1146871/messages"

    # Set authorization header with token
    headers = {
        "Authorization": "15APdQKKk81AbhH5oBfp4MfzRg72z9tq2S34yBEJyw9xBEtP6y5Ya8oPflFEiZTa"
    }

    # last_timestamp = None


    # Send GET request to API with headers
    response = requests.get(url, headers=headers)

    data= response.json()
    data_res=data['result']
    # print(data)


    # Check if request was successful
    if response.status_code == 200:

        try:
            # Connect to MySQL database
            conn = mysql.connector.connect(user='root', password='', host='localhost', database='teltonika')

            # Get cursor to perform operations on database
            cursor = conn.cursor()


            for data_n in data_res:
                
                keys=list(data_n.keys())
                # print(keys)
                # vals=list(data_n.values())
                
                column_name_list=[]
                column_vals=[]

                for key in keys:
                    column_name_list.append(key.replace('.','_'))

                # print(column_name_list)

                
                
                
                for i,j in zip(column_name_list, keys):
                    i= data_n[f'{j}']
                    column_vals.append(i)
                    # print(f'{i} is assigned to {j}')

                # print(column_vals)

                delimiter = ", "
                columns_string = delimiter.join(column_name_list)
                # print(columns_string)

                # print(len(keys), len(column_name_list), len(column_vals))
                


                sql_insert_query= f"INSERT INTO teltonikas({columns_string}) VALUES ("

                for i in range(len(column_name_list)):
                    sql_insert_query += '%s, '
                
                # remove the last comma and add closing bracket
                sql_insert_query = sql_insert_query[:-2]
                sql_insert_query += ")"

                insert_tuple = tuple(column_vals)
                cursor.execute(sql_insert_query, insert_tuple)
                
                # check for missing data
                if cursor.rowcount == 0:
                    print("Failed to insert record into teltonikas table, missing data")
                else:
                    # Commit changes to database
                    conn.commit()
                    print("Data successfully inserted!")

                

        except mysql.connector.Error as error:
            print("Failed to insert record into teltonikas table {}".format(error))
        # Close cursor and connection to database
        finally:
            if (conn.is_connected()):
                cursor.close()
                conn.close()
                print("MySQL connection is closed")
            
    else:
        # Print error message if request was not successful
        print("An error occurred: {}".format(response.status_code))

    
except requests.exceptions.RequestException as e:
    print("An Error Occured: {}".format(e))
except ValueError as ve:
    print("Error Occured: {}".format(ve))