
import os 
import env
import datetime
from ftplib import FTP
from urllib.parse import urlparse
from dateutil import parser
import pandas as pd
import numpy as np

FTP_HOST =  env.host
FTP_USER = env.user
FTP_PASS = env.password

def check_FTP_connection():
    try:
        parsed = urlparse(FTP_HOST)
        ftp = FTP(parsed.netloc)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(parsed.path)
        print("connection established successfully.")
        return ftp
    except:
        print("error while connecting FTP server.")
        return False


def get_latest_File():
    try:
        lines = []
        file_name= []
        last_modified = []
        connection_status = check_FTP_connection()
        if not connection_status == False:
            ftp = connection_status
            parsed = urlparse(FTP_HOST)
            ftp.dir(parsed.path, lines.append)
            # files_list = ftp.nlst()
            # print(lines)
        for line in lines:
            tokens = line.split(maxsplit = 9)
            name = tokens[8]
            time_str = tokens[5] + " " + tokens[6] + " " + tokens[7]
            time = parser.parse(time_str)
            file_name.append(name)
            last_modified.append(str(time))
        file_table = pd.DataFrame(list(zip(file_name, last_modified)),
               columns =['File_Name', 'Last_Modified_Date'])

        file_table['last_run_date'] = pd.to_datetime(env.last_run_date)
        file_table['Last_Modified_Date'] = pd.to_datetime(file_table['Last_Modified_Date'])

        file_table['latest_files'] = np.where((file_table['Last_Modified_Date'] > file_table['last_run_date']),file_table['File_Name'], '')
        
        filtered_files = file_table.loc[file_table['latest_files'] !='']
        output_table = filtered_files[['File_Name','Last_Modified_Date']]
        latest_files = filtered_files['latest_files'].tolist()
        file_categories = [file.split('.')[0] for file in latest_files if file.endswith('.zip')]
        return output_table, file_categories
    except Exception as e:
        raise e

# check_FTP_connection()
lat_files, cat = get_latest_File()
print(lat_files)
print(cat)

