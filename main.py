import json
import yfinance as yf
import os
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import boto3
import csv
import zipfile
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine

#Set Directory
dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)
cwd = os.getcwd()

#Import credentials
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

#Import historical closing price and Key company Metrics
def historical_price(company):

    stock_info_df = pd.DataFrame(columns= ["Company","Total Cash","Total Revenue", "Total Debt","Debt to Equity","Revenue Growth","Gross Profits", "Market Cap", "Profit Margins"])
    stock_df = pd.DataFrame()
    count = 0
    for x in company:
        comp_price = yf.Ticker(x)
        
        stock_info_df.loc[count] = [x, comp_price.info["totalCash"], comp_price.info["totalRevenue"], comp_price.info["totalDebt"], comp_price.info["debtToEquity"], comp_price.info["revenueGrowth"], comp_price.info["grossProfits"], comp_price.info["marketCap"], comp_price.info["profitMargins"]]

        hist_price = comp_price.history(period="6mo")
        stock_df[x] = hist_price["Close"]
        count = count + 1
    stock_df["Date"] = stock_df.index
    return  stock_df, stock_info_df    



def push_to_s3(stock_price_data, stock_comp_info, c_dir):

    aws_cred = credentials_check()
    client = boto3.client(
        "s3",
        aws_access_key_id= aws_cred[1][:-1],
        aws_secret_access_key = aws_cred[2][:-1],
        region_name= "us-west-2"
    )

    #Export stock data to zipped csv file then push it to S3
    with zipfile.ZipFile('stock_price_complete.zip', 'w') as zip_file:
        zip_file.writestr('stock_price_data.csv', stock_price_data.to_csv(index=False))
        zip_file.writestr('stock_info_data.csv', stock_comp_info.to_csv(index=False))
    try:
        client.upload_file(c_dir + "/stock_price_complete.zip", "stock-market-bucket1", "stock_price_complete.zip")
    except:
        err_msg = "File upload to S3 Error"
    else:
        err_msg = "File upload to S3 Successful"
    return err_msg


#Create MySQL price history table in RDS and push into mysql table
def mysql_push(stock_data, stock_info):
    #Retrieve credentials
    aws_cred = credentials_check()
    print(aws_cred)
    #Connect to mysql database
    sql_connection = mysql.connector.connect(host = aws_cred[3][:-1], user = aws_cred[4][:-1], password = aws_cred[5][:-1], database = "stock-market-db1")
    cursor = sql_connection.cursor()
    #print("error connecting to db")
    
    
    
    col_names = ""
    #Create mysql table
    for cols in stock_data.columns[:-1]:
        col_names = col_names +"`"+cols+"`" + " float NULL,"
    create_stock_price_table = """CREATE TABLE IF NOT EXISTS `stock_price_data` (
         `key_id` int NOT NULL AUTO_INCREMENT,
         """ + col_names + """ `Date` datetime NOT NULL, PRIMARY KEY(key_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1;"""
    
    cursor.execute(create_stock_price_table)
    sql_connection.commit()

    new_dataframe = stock_data.copy()

    creds = {
        'usr': aws_cred[4][:-1],
        'pwd': aws_cred[5][:-1],
        'hst': aws_cred[3][:-1],
        'prt': 3306, 
        'dbn': aws_cred[6][:-1]
    }

    al_connect = 'mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}'

    engine = create_engine(connstr.format(**creds))
    chunk = int(len(new_dataframe)/1000)
    try:
        new_dataframe.to_sql(name = "stock_price_data", con= engine, if_exists="replace", chunksize=chunk, index=False)
    except:
        print("Load to Dataframe Error")
    else:
        print("Dataframe load Successfull")


#Create dataframe with given company codes
companies = ["AAPL", "META", "TSLA", "GOOG", "NVDA"]
stock_data, stock_info  = historical_price(companies)

#Push the dataframe into AWS S3
#push_to_s3(stock_data, stock_info,cwd)

#Push data into mysql
#mysql_push(stock_data, stock_info)


#plt.plot(stock_data[])


#print(stock_data.iloc[:,0])

plt.plot(stock_data["Date"],stock_data.iloc[:,0], label = "AAPL")
plt.plot(stock_data["Date"],stock_data.iloc[:,1], label = "META")
plt.plot(stock_data["Date"],stock_data.iloc[:,2], label = "TSLA")
plt.plot(stock_data["Date"],stock_data.iloc[:,3], label = "GOOG")
plt.plot(stock_data["Date"],stock_data.iloc[:,4], label = "NVDA")
plt.legend(loc = "upper left")
plt.title("Stock Price in the past 6 months")
plt.show()