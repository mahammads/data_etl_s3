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
import gzip, shutil


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
def unzip(file):
    try:
        file_extension = file.split('.')[-1]
        file_folder = (os.path.basename(file)).rsplit('.',1)[0]
        file_name = os.path.join(env.local_file_folder, file)
        extracted_file_path = os.path.join(env.unzip_folder, file_folder)
        if not os.path.exists(extracted_file_path):
            os.makedirs(extracted_file_path)

        if file_extension == 'zip':# check for ".zip" extension
            with zipfile.ZipFile(file_name,"r") as zip_ref:
                zip_ref.extractall(extracted_file_path)
            # os.remove(file_name) # delete zipped file
        extracted_file_name = extracted_file_path
        if file_extension == 'gz': # check for ".gz" extension
            updated_name = (os.path.basename(file_name)).rsplit('.',1)[0]
            extracted_file_name = os.path.join(extracted_file_path,updated_name)
            with gzip.open(file_name,"rb") as f_in, open(extracted_file_name,"wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            # os.remove(file_name) # delete zipped file

        print("file unzip successfully")
        return extracted_file_name
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
    # download_FTP()
    list_zip_files = os.listdir(env.local_file_folder)
    if len(list_zip_files)!= 0:
        bucket_name = env.s3_bucket_name
        s3_output_folder = env.s3_folder
        today_date = date.today()
        d1 = today_date.strftime("%d-%m-%Y")
        for zip_f in list_zip_files:
            file_folder = (os.path.basename(zip_f)).rsplit('.',1)[0]
            s3_folder_name = s3_output_folder + '/' +d1+ '/' +  file_folder
            extract_file_path = unzip(zip_f)

            for file in os.listdir(extract_file_path):
                local_file_name = os.path.join(extract_file_path, file)
                s3_file_name =s3_folder_name +'/'+ file
                try:
                    uploaded = upload_to_aws(local_file_name, bucket_name, s3_file_name)
                except IsADirectoryError:
                    for sub_file in os.listdir(local_file_name):
                        sub_file_name = os.path.join(local_file_name, sub_file)
                        sub_s3_file_name =s3_file_name +'/'+ sub_file
                        uploaded = upload_to_aws(sub_file_name, bucket_name, sub_s3_file_name)
            print(f"{file} file uploaded successfully")
    print("all file uploaded successfully")
    t1 = time.time()
    total = t1-t0
    print(f"total time taken: {total}")

if __name__ == "__main__":
    process()
