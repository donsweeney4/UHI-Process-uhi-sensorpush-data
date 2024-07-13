"""

Filename: ProcessCSVtoSQL_3.py

Processing:

connect to mysql database uhi with username uhi and password uhi     
open each .csv file in /home/uhi/SensorData 
cycle through each line of the opened file
first two lines of each file are headers.  
From the filename extract the first 8 ascii characters. The eight characters are the sensorid.   
Beginning with the third line, read the the three fields: timestamp, temperature as a float, humidity as float
insert into the database table sensor_data the four fields: sensorid, timestamp, temperature, humidity.  
if there is a duplication of the composite primary key, update the data entry

Frequency:



"""




import os
import csv
import shutil
import time
import mysql.connector
from mysql.connector import Error

# Define the directory containing the CSV files
sensor_data_dir = "/home/uhi/SensorData"
archive_dir = "/home/uhi/archived_data"
# Function to connect to MySQL database
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='uhi',
            user='uhi',
            password='uhi'
        )
        if connection.is_connected():
            print("Connected to MySQL database")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# Function to insert data into the sensor_data table
def insert_data(cursor, sensorid, timestamp, temperature, humidity,pressure):
    if pressure == -1:
        insert_query = """INSERT INTO sensor_data (sensorid, timestamp, temperature, humidity)
                      VALUES (%s, %s, %s, %s)
                      ON DUPLICATE KEY UPDATE
                      temperature = VALUES(temperature),
                      humidity = VALUES(humidity)"""    
        record = (sensorid, timestamp, temperature, humidity)
    if pressure != -1:
        insert_query = """INSERT INTO sensor_data (sensorid, timestamp, temperature, humidity,pressure)
                      VALUES (%s, %s, %s, %s, %s)
                      ON DUPLICATE KEY UPDATE
                      temperature = VALUES(temperature),
                      humidity = VALUES(humidity),
                      pressure = VALUES(pressure)"""   
        record = (sensorid, timestamp, temperature, humidity, pressure)


    cursor.execute(insert_query, record)

# Function to process each CSV file and insert data into the database
def process_files(connection):
    cursor = connection.cursor()
    for filename in os.listdir(sensor_data_dir):
        if filename.endswith(".csv"):
            file_path = os.path.join(sensor_data_dir, filename)
            
            # Extract sensorid from the first 8 characters of the filename
            sensorid = filename[:8]
            print(f"Processing file: {filename} with sensorid: {sensorid}")
            
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                csvreader = csv.reader(csvfile)
                lines = list(csvreader)
                if len(lines) < 3:
                    print(f"File {filename} does not contain enough data")
                    continue

                # Process each line starting from the third line
                for line in lines[2:]:
                    if len(line) < 3:
                        print(f"Line missing data in {filename} --> length: {len(line)}")
                    else:
                        timestamp = line[0]
                        temperature = float(line[1])
                        humidity = float(line[2])
                        pressure = -1
                        if len(line) == 4:
                            pressure = float(line[3])
                        insert_data(cursor, sensorid,timestamp,temperature,humidity,pressure)
# Generate a new filename with the current Unix timestamp
            timestamp = int(time.time())
            new_filename = f"{filename[:-4]}_{timestamp}.csv"
            archive_path = os.path.join(archive_dir, new_filename)
# Move the processed file to the archive directory
            archive_path = os.path.join(archive_dir, filename)
            shutil.move(file_path, archive_path)
            print(f"Moved {filename} to {archive_path}")



    connection.commit()
    cursor.close()

# Main function
def main():
    connection = connect_to_database()
    if connection is not None and connection.is_connected():
        process_files(connection)
        connection.close()

if __name__ == "__main__":
    main()

