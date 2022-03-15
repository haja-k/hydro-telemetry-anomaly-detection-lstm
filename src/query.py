import pandas as pd 
pd.options.mode.chained_assignment = None 

import json
import os

import mysql.connector 

def main(year_int, name):
    credential_val = credential()
    hostname = credential_val[0]
    username = credential_val[1]
    password = credential_val[2]
    database_name = credential_val[3]
    port = credential_val[4]
    
    try:
        year = str(year_int)
        date_start = year + "-01-01 00:00:00"
        date_end = str(year_int+1) + "-01-01 00:00:00"
        station_name = name    
        start_date = date_start.split()[0]
        end_date = date_end.split()[0] 
        
        db_connection = mysql.connector.connect(user=username, password=password, host=hostname, database=database_name,
                                                port=port)
        print("Database connection established")
        
        df_data = getData(db_connection, date_start, date_end, station_name)
        print(df_data.dtypes)
        print("a) Original data length: ", len(df_data))
        
        df_duplicated_data = df_data[df_data.duplicated()]
        print("b) Duplicated data length:", len(df_duplicated_data))

        df_unique_data = df_data.drop_duplicates()
        print("c) Unique data length:", len(df_unique_data))

        df_unique_data['year_month'] = pd.to_datetime(df_unique_data['timestamp']).dt.strftime('%Y-%m')
        
        # make directory
        directory = "dataset/" + name + "/"
        filename = directory + station_name + "_" + year + ".csv"
        if not os.path.exists(directory):
            os.makedirs(directory)
            
        df_unique_data.to_csv(filename, sep=',')
        db_connection.close()
    except FileNotFoundError as fnf_error:
        print('FILE NOT FOUND ERROR --> ', fnf_error)
    except Exception as err:
        print(err)
        
def credential():
    credential_file = open('../src/credential.json', "r", encoding='utf-8')
    credential = json.load(credential_file)
    hostname = credential['DEV']['hostname']
    username = credential['DEV']['username']
    password = credential['DEV']['password']
    database_name = credential['DEV']['database_name']
    port = credential['DEV']['port']
    return hostname, username, password, database_name, port

def getData(db_connection, date_start, date_end, station_name):
    sql = """
            SELECT a.StationID as station_id, b.station_desc as station_desc, a.Timestamp as timestamp, a.AI1 as ai1, b.amsl as amsl, a.AI1 + b.amsl as actual_reading, a.DI1Cnt as rainfall
            FROM telemetry.reading a inner join telemetry.t_station b on a.StationID = b.station_id
            WHERE a.timestamp >= %s and a.timestamp < %s and b.station_desc = %s 
        """
        
    cursor = db_connection.cursor()
    cursor.execute(sql, (date_start, date_end, station_name))
    result_set = cursor.fetchall()
    field_names = [i[0] for i in cursor.description]
    df_data = pd.DataFrame(result_set, columns=field_names)
    
    return df_data

if __name__ == "__main__":
    for i in range(2016, 2022):
        main(i, 'Beluru')