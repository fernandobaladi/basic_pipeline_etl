# Python Pipeline!

Welcome! To this new pipeline that I made. This pipeline is a based in an ETL process, where it extracts information of a CSV, it transform the information and then loads it in a PostreSQL database. Please read it carefully and to the end.


## Requirements 

 - Python3
 - Psycopg2
 - Pandas
Also you have to download the data set Used Cars Dataset of Austin Reese:
https://www.kaggle.com/datasets/austinreese/craigslist-carstrucks-data

Once you download the dataset and install the necessary libs, you will be able to execute it. 

## Execute the file
To execute the file by shell, you have to pass two arguments:

 - The first one should be the complete path of the dataset
 - The second one is should be a number from 0 to 2, where:
	 - If you pass 0, you will add the information to the database (Default)
	 - If you pass 1, you will delete all the information in the table
	 - If you pass 2, you will download all the information in the table

Example:
     `> python3 pipeline.py "your_path\vehicles.csv" 1`


## About the dataset

The dataset is the Used Cars Dataset of Austin Reese that you can find in Kaggle.
The dataset that you will pass to the algorithm should be in CSV format. And should has at least the next parameters:

 - id (This will be the same ID in the database)
 - condition
 - cylinders
 - fuel
 - odometer
 - description
 - price
 - year
 - manufacturer
 - title_status
 - VIN
 - transmission
 - drive
 - size
 - type
 - paint_color
 - state
 - model
 - posting_date
### Warning!!!!
This dataset is 1.5GB after unzipping, and during the execution it will only upload the first 1001 rows that it finds, the other rows will be discarded. Also I added a really small sub-dataset of the original one. You are free to use it.

## Database

The database is a PostgreSQL database hosted in ElephantSQL with a maximum storage of 20MB. And all the queries are based in PostgreSQL.
 
## Logs file

If there is error, the pipeline will create a folder called "logs" with a file called "algorithm_errors.log" where all the log errors will be shown.

## Delete database information

If you want to delete the information of the table, you can pass the number 1 as the second parameter. It will "clean" all the table.

## Export database information

If you want to delete the information of the table, you can pass the number 2 as the second parameter. It will download all the table and stored it a folder called "exports" with a CSV file called after current timestamp.