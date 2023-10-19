import pandas as pd
import boto3
import csv
import zipfile
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import os

#Set Directory
dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)
cwd = os.getcwd()

def credentials_check():
    key_file  = pd.read_csv("credentials.csv")
    file_path = key_file.iloc[0,0]
    aws_key = key_file.iloc[0,1]
    aws_secret_key = key_file.iloc[0,2]
    rds_host = key_file.iloc[0,3]
    rds_user = key_file.iloc[0,4]
    rds_password = key_file.iloc[0,5]
    rds_database = key_file.iloc[0,6]
    cred_list = [file_path, aws_key, aws_secret_key, rds_host, rds_user, rds_password, rds_database]
    return cred_list


aws_cred = credentials_check()


sql_connection = mysql.connector.connect(host = aws_cred[3][:-1], user = aws_cred[4][:-1], password = aws_cred[5][:-1], database = "stock-market-db1")
cursor = sql_connection.cursor()