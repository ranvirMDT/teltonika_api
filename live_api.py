import requests
import json
import mysql.connector
import time


# NOTE: Script requires table with the columns that have the same name as JSON data key but with "_" instead of "." 


# Set timestamp as current time
last_timestamp = int(time.time())
# print(last_timestamp)

# API url
url = "https://flespi.io/gw/channels/1149052/messages"

# Set authorization header with token
headers = {
    "Authorization": "9OTHeaQjRDAbIku4mDIU25SoYsGga2kaG1HIZlSHOiIlq1fAuLllG1zuh5yiXHgs"
}

max_retries = 10
retry_count = 0

while True:
    try:
    # last_timestamp = int(time.time())

    

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

                # iterate through each data row
                for data_n in data_res:
                    timestamp = data_n['timestamp']
                    
                    # Only proccess latest data
                    if timestamp > last_timestamp:
                        print(data_n)
                        keys=list(data_n.keys())
                        # print(keys)
                        # vals=list(data_n.values())
                        
                        column_name_list=[]
                        column_vals=[]

                        # save column names as keys but using "_" instead of "." 
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

                        # Execute SQL insert query with data values
                        insert_tuple = tuple(column_vals)
                        cursor.execute(sql_insert_query, insert_tuple)
                        
                        # check for missing data
                        if cursor.rowcount == 0:
                            print("Failed to insert record into teltonikas table, missing data")
                        else:
                            # Commit changes to database
                            conn.commit()
                            print("Data successfully inserted!")
                            last_timestamp= timestamp
                    else:
                        pass
                    

            except mysql.connector.Error as error:
                print("Failed to insert record into teltonikas table {}".format(error))
            # Close cursor and connection to database
            finally:
                if (conn.is_connected()):
                    cursor.close()
                    conn.close()
                    # print("MySQL connection is closed")
                    retry_count = 0
                
        else:
            # Print error message if request was not successful
            print("An error occurred: {}".format(response.status_code))
            print(response.text)

        
    except requests.exceptions.RequestException as e:
        print("An Error Occured: {}".format(e))
        retry_count += 1
        print(f"Error: {e}. ")
        print("Retrying in 30 seconds...")
        if retry_count >= max_retries:
            print("Max retries exceeded!")
            break
        time.sleep(30)

    except ValueError as ve:
        print("Error Occured: {}".format(ve))

    finally:
        time.sleep(360)

