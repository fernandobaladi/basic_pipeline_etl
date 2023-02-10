# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import psycopg2
import pandas as pd
from datetime import datetime
import logging
import os
import sys

#Logs file
log_file_path = r'./logs/pipeline_errors.log'
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

#Let's define some constants
host='baasu.db.elephantsql.com'
user ='itdijebq'
password='ePoij4o6oUmdLz0a8IDlyGdAAfeY_3AB'
dbname='itdijebq'
try:
    myConnection = psycopg2.connect(host = host,
                                user= user, password =password,
                                dbname= dbname)
except Exception as e:
    logger.error(e)
    print('There was an error creating the connection with the database')
    print('For more information, check the logs file')
    

def myQuery(query):
    cur = myConnection.cursor()
    try:
        cur.execute(query)
    except Exception as e:
        myConnection.commit()
        logger.error(e)
        print('There was an error in the query')
        print('For more information, check the logs file')
    else:
        cur.close()
        myConnection.commit()
        return 1
        
def select(query):
    cur = myConnection.cursor()
    try:
        cur.execute(query)
    except Exception as e:
        myConnection.commit()
        logger.error(e)
        print('There was an error in the query')
        print('For more information, check the logs file')
        
    else:
        records = cur.fetchall()
        cur.close()
        return records

def set_database():
    myQuery("""CREATE TABLE IF NOT EXISTS cars_sales (
	id VARCHAR ( 50 ) UNIQUE NOT NULL PRIMARY KEY,
    model INTEGER NOT NULL,
    condition VARCHAR ( 50 ) NOT NULL,
    cylinders VARCHAR ( 50 ) NOT NULL,
    fuel VARCHAR ( 50 ) NOT NULL,
	odometer INTEGER NOT NULL,
	description TEXT NOT NULL,
	price INTEGER NOT NULL,
	year INTEGER NOT NULL,
	manufacturer VARCHAR ( 50 ) NOT NULL,
	title_status VARCHAR ( 50 ) NOT NULL,
	VIN VARCHAR ( 50 ) NOT NULL,
	transmission VARCHAR ( 50 ) NOT NULL,
	drive VARCHAR ( 50 ) NOT NULL,
	size VARCHAR ( 50 ) NOT NULL,
	type VARCHAR ( 50 ) NOT NULL,
	paint_color VARCHAR ( 50 ) NOT NULL,
	state VARCHAR ( 50 ) NOT NULL,
	posting_date TIMESTAMPTZ NOT NULL,
	year_sale INTEGER NOT NULL

    )""")

def clean_database():
    myQuery("DELETE FROM cars_sales")

def get_database():
    result = select('SELECT * FROM "public"."cars_sales"')
    os.makedirs(os.path.dirname('./exports/'), exist_ok=True)
    pd.DataFrame(result).to_csv('./exports/'+str(datetime.strftime(datetime.now(),"%Y_%m_%dT%H_%M_%S",))+'.csv')
    print('All the data from the database is now in your exports folder')
    
def __init__():
    try:
        sys_args = sys.argv

        if(len(sys_args)==1):
            raise Exception("You didn't provide enough arguments")
        else:
            
            if(len(sys_args)==2):
                sys_args.append(0) 
            if(int(sys_args[2]) == 0):
                file_path = r''+ sys_args[1]
                #Let's create the data base if it's not created yet
                set_database()
                transforming = ''
                try:
                    transforming = pd.read_csv(file_path)
                except Exception as e:
                    logger.error(e)
                    print('There was an error loading the file.\nPlease, remember that the file should be a CSV file')
                    print('For more information, check the logs file')
                
                #Data Cleaning
                try:
                    transforming_droped_columns = transforming.loc[:,['id', 'condition', 
                                                                    'cylinders', 'fuel',
                                                                    'odometer', 'description', 
                                                                    'price', 'year',
                                                                    'manufacturer','title_status',
                                                                    'VIN', 'transmission',
                                                                    'drive', 'size',
                                                                    'type', 'paint_color',
                                                                    'state', 'model', 'posting_date']]
                    transforming_clean = transforming_droped_columns.dropna().reset_index().loc[:1000,:].copy(deep=True)
                
                    #data transformation
                    
                    #Here we get the year of sale
                    transforming_clean.loc[:,'year_sale'] = transforming_clean['posting_date'].apply(lambda x: datetime.strptime(x,"%Y-%m-%dT%H:%M:%S%z").year).to_list()
                    #Here we tranform the date of the posting
                    transforming_clean.loc[:, 'posting_date'] = transforming_clean['posting_date'].transform(lambda x: datetime.strptime(x,"%Y-%m-%dT%H:%M:%S%z"))
                    
                    #Cleaning the description
                    transforming_clean.loc[:, 'description'] = transforming_clean['description'].transform(lambda x: x.replace('"', "\"").replace("'", "''"))
                    
                    #model enum is a list with the car's model of the dataset
                    model_enum = transforming_clean['model'].unique()
                    model_enum = list(model_enum)
                    # "indexing" car models
                    transforming_clean.loc[:, 'model_index'] = transforming_clean['model'].transform(lambda x: model_enum.index(x))
                    
                    #Here we set the string that will be used to send the query
                    transforming_clean.loc[:, 'value_insert'] = transforming_clean.apply(lambda row: """('"""+str(row['id'])+str(datetime.strftime(datetime.now(),"_%Y_%m_%dT%H_%M_%S",))+"""',
                                                        """+str(row['model_index'])+""",
                                                        '"""+str(row['condition'])+"""',
                                                        '"""+row['cylinders']+"""',
                                                        '"""+row['fuel']+"""',
                                                        """+str(int(row['odometer']))+""",
                                                        '"""+row['description']+"""',
                                                        """+str(row['price'])+""",
                                                        """+str(int(row['year']))+""",
                                                        '"""+row['manufacturer']+"""',
                                                        '"""+row['title_status']+"""',
                                                        '"""+row['VIN']+"""',
                                                        '"""+row['transmission']+"""',
                                                        '"""+row['drive']+"""',
                                                        '"""+row['size']+"""',
                                                        '"""+row['type']+"""',
                                                        '"""+row['paint_color']+"""',
                                                        '"""+row['state'].upper()+"""',
                                                        '"""+str(row['posting_date'])+"""',
                                                        """+str(int(row['year_sale']))+""");
                                                        """, axis=1)
                                                        
                    for index, row in transforming_clean.iterrows():
                        
                        insert_sale = """INSERT INTO cars_sales ("id", "model", "condition", "cylinders", "fuel", "odometer", "description", "price", "year", "manufacturer", "title_status", "vin", "transmission", "drive", "size", "type", "paint_color", "state", "posting_date", "year_sale")
                                                values """ + row['value_insert']
                        myQuery(insert_sale)
                except Exception as e:
                    logger.error(e)
                    print('There was an error in the transformation of the data.')
                    print('For more information, check the logs file')
            elif(int(sys_args[2])== 1):
                clean_database()
            elif(int(sys_args[2])== 2):
                get_database()
            else:
                raise Exception("Unknown value for parameter. Please, select 0, 1 or 2")
    except Exception as e:
        logger.error(e)
        print('There was an error in the execution')
        print('For more information, check the logs file')
    
__init__()