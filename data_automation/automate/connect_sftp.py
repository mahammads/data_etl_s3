import pysftp as sftp
import os 
import boto3
from botocore.exceptions import NoCredentialsError
import env
from datetime import date
import zipfile

FTP_HOST =  env.host
FTP_USER = env.user
FTP_PASS = env.password
port=port = env.port

def download_sftp():
   cnopts = sftp.CnOpts()
   cnopts.hostkeys = None
   try:
      with sftp.Connection(host=FTP_HOST,port=port,username=FTP_USER, password=FTP_PASS, cnopts=cnopts) as serv_details:
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
            serv_details.get(remote_file, local_file)
            print(file_name,'downloaded successfully')
   except Exception as e:
      raise e

def unzip():
    try:
        list_zip_files = os.listdir(env.local_file_folder)
        if len(list_zip_files)!= 0:
            for file in list_zip_files:
                file_extension = file.split('.')[-1]
                file_folder = file.split('.')[0]
                file_name = os.path.join(env.local_file_folder, file)
                extracted_file_path = os.path.join(env.unzip_folder, file_folder)
                if not os.path.exists(extracted_file_path):
                    os.makedirs(extracted_file_path)

                if file_extension == '.zip':
                    with zipfile.ZipFile(file_name,"r") as zip_ref:
                        zip_ref.extractall(extracted_file_path)
                # os.remove(file_name)
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

def process():
    download_sftp()
    zip_status = unzip()
    files_path = env.unzip_folder
    bucket_name = env.s3_bucket_name
    s3_output_folder = env.s3_folder
    today_date = date.today()
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

if __name__ == "__main__":
    process()

