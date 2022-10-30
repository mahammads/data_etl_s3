# for FTP server:
host = ''
user = ''
password = ''
port = 22

s3_bucket_name = ''
access_key = ''
secret_key = ''
region = ''
local_file_folder = 'files'
s3_folder = 'zip_files'
remote_path = 'document'
unzip_folder = 'extracted_files'
temp_file_list = ['','']
root_folder = '/home/jubers/data_etl'

temp_flag = True
# https://github.com/mahammads/data_etl_s3.git
# 45 14 * * * /home/jubers/virtual_env/test/bin/python3 /home/jubers/data_etl/connect_FTP.py > /var/log/cronlog 2>&1 
