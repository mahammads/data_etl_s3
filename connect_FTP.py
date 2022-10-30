import pysftp as sftp
import os 
import boto3
from botocore.exceptions import NoCredentialsError
import env
from datetime import date
import zipfile
from ftplib import FTP
from urllib.parse import urlparse
import time

FTP_HOST =  env.host
FTP_USER = env.user
FTP_PASS = env.password

sftp_host =  env.host
sftp_user = env.user
sftp_pw = env.password
port=port = env.port

# function to get the file from ftp server.
def getFile(ftp, filename):
    try:
        local_file_path = env.local_file_folder +'/' + filename
        ftp.retrbinary("RETR " + filename ,open(local_file_path, 'wb').write)
        return True
    except:
        print("Error")
        return False
     

# function for donwloading all the files present in ftp server remote directory.
def download_FTP():
    try:
        parsed = urlparse(FTP_HOST)
        print(parsed)
        ftp = FTP(parsed.netloc)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(parsed.path)
        print("connection established successfully")
        files_list = ftp.nlst()
        if env.temp_flag:
            files_list = env.temp_file_list
        for file_name in files_list:
            getFile(ftp, file_name)
            print(file_name,'downloaded successfully')
    except Exception as e:
      raise e

# function for donwloading all the files present in sftp server remote directory.
def download_sftp():
   cnopts = sftp.CnOpts()
   cnopts.hostkeys = None
   try:
      with sftp.Connection(host=sftp_host,port=port,username=sftp_user, password=sftp_pw, cnopts=cnopts) as serv_details:
        print("connection established successfully")
        current_dir = serv_details.pwd
        print(current_dir)
        remoteFileLoc = current_dir + '/' + env.remote_path
        local_file_path = env.local_file_folder
        with serv_details.cd(remoteFileLoc):
            files_list = serv_details.listdir()
        # print(files_list)
        if env.temp_flag:
            files_list = env.temp_file_list
        for file_name in files_list:
            remote_file = remoteFileLoc + '/' + file_name
            local_file = local_file_path + '/' + file_name
            download_status = serv_details.get(remote_file, local_file)
            if download_status:
                print(file_name,'downloaded successfully.')
            else:
                print("error in downloading file.")
   except Exception as e:
      raise e

# function for unzipping the all zip files present in respective folder.
def unzip():
    try:
        list_zip_files = os.listdir(env.local_file_folder)
        if len(list_zip_files)!= 0:
            for file in list_zip_files:
                file_folder = file.split('.')[0]
                print(file_folder)
                file_name = os.path.join(env.local_file_folder, file)
                with zipfile.ZipFile(file_name,"r") as zip_ref:
                    extracted_file_path = os.path.join(env.unzip_folder, file_folder)
                    if not os.path.exists(extracted_file_path):
                        os.makedirs(extracted_file_path)
                    zip_ref.extractall(extracted_file_path)
                os.remove(file_name)
                print("file unzip successfully")
        else:
            print("no zip file found to unzip")
    except Exception as e:
        raise e
        # return False

ACCESS_KEY = env.access_key
SECRET_KEY = env.secret_key
region = env.region

s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, region_name =region)

# function for uploading data to amazon S3 bucket using boto3.
def upload_to_aws(local_file, bucket, s3_file):
    
    try:
        s3.upload_file(local_file, bucket, s3_file)
        # print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

# consolidating all the funcitons.
def process():
    t0 = time.time()
    download_FTP()
    zip_status = unzip()
    extracted_folder = env.unzip_folder
    bucket_name = env.s3_bucket_name
    s3_output_folder = env.s3_folder
    today_date = date.today()
    root_dr = os.getcwd()
    files_path = root_dr + '/' + extracted_folder
    d1 = today_date.strftime("%d-%m-%Y")
    for file in os.listdir(files_path):
        local_folder_name = os.path.join(files_path, file)
        s3_folder_name = s3_output_folder + '/' +d1+ '/' +  file

        for file in os.listdir(local_folder_name):
            local_file_name = os.path.join(local_folder_name, file)
            s3_file_name =s3_folder_name +'/'+ file
            try:
                uploaded = upload_to_aws(local_file_name, bucket_name, s3_file_name)
            except IsADirectoryError:
                for file in os.listdir(local_file_name):
                    sub_file_name = os.path.join(local_file_name, file)
                    sub_s3_file_name =s3_file_name +'/'+ file
                    uploaded = upload_to_aws(sub_file_name, bucket_name, sub_s3_file_name)
        print(f"{local_folder_name} file uploaded successfully")
    print("all file uploaded successfully")
    t1 = time.time()
    total = t1-t0
    print(f"total time taken: {total}")

if __name__ == "__main__":
    process()
