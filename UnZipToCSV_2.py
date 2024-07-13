"""
File: UnZipToCSV.py 

Processing:

find .zip files in /home/uhi/email_attachments  

Files of interest contain a part of their name with SensorPush.  

Parse through the directory and unzip the files, 

save them in a directory /home/uhi/SensorData as .csv files

print the first 10 lines of each file.


Frequency:


"""
import os
import zipfile
import csv

# Define directories
attachments_dir = "/home/uhi/email_attachments"
sensor_data_dir = "/home/uhi/SensorData"

# Ensure the SensorData directory exists
if not os.path.exists(sensor_data_dir):
    os.makedirs(sensor_data_dir)

# Function to unzip files and save CSV contents
def unzip_and_process_files():
    for filename in os.listdir(attachments_dir):
        if "SensorPush" in filename and filename.endswith(".zip"):
            zip_path = os.path.join(attachments_dir, filename)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Extract all files to the SensorData directory
                zip_ref.extractall(sensor_data_dir)
                for file_info in zip_ref.infolist():
                    if file_info.filename.endswith(".csv"):
                        csv_path = os.path.join(sensor_data_dir, file_info.filename)
                        print(f"Extracted {csv_path}")
                        print_first_10_lines(csv_path)
# Delete the zip file after processing
            os.remove(zip_path)
            print(f"Deleted {zip_path}")


# Function to print the first 10 lines of a CSV file
def print_first_10_lines(file_path):
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        for i, row in enumerate(csvreader):
            if i >= 10:
                break
            print(', '.join(row))
        print("\n")

# Run the function
unzip_and_process_files()
