
import csv
import pyodbc

def load_csv_data(file_path, table_name, connection_string):

    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    cursor.fast_executemany=True

    with open(file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        header = csv_reader.fieldnames
        data = [tuple(row.values()) for row in csv_reader]


    placeholders = ', '.join(['?' for _ in header])
    sql_query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({placeholders})"

    cursor.executemany(sql_query, data)

    connection.commit()

    cursor.close()
    connection.close()




def load_incident(file_path, table_name, connection_string):
 
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    cursor.fast_executemany=True


    with open(file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        header = csv_reader.fieldnames

 
        placeholders = ', '.join(['?' for _ in header])
        sql_query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({placeholders})"

        data = [tuple(row[column] for column in header) for row in csv_reader]


    cursor.executemany(sql_query, data)

    connection.commit()

    cursor.close()
    connection.close()



if __name__ == "__main__":

    connection_string = 'DRIVER={SQL Server};SERVER=tcp:lds.di.unipi.it;DATABASE=Group_ID_32_DB;UID=Group_ID_32;PWD=20RNW0GN'
    try: 
        # start loading data
        load_csv_data('dates.csv', 'dates', connection_string)

        load_csv_data('gun.csv', 'gun', connection_string)

        load_csv_data('partecipant.csv', 'partecipant', connection_string)

        load_incident('incident.csv', 'incident', connection_string)    

        load_csv_data('geography.csv', 'geography', connection_string)

        # custody has to be the last table we populate because of data dependecies due to FKs
        load_csv_data('custody.csv', 'custody', connection_string)
        
    except Exception as e:
        print("error in the overall process")





