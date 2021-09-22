import pandas as pd
import datetime
from datetime import datetime,timedelta
from pymongo import MongoClient
import mysql
from mysql.connector import Error


CLIENT = MongoClient('mongo_airflow',27017, maxPoolSize=50)
DB = CLIENT['spotify']
COLLECTION = DB['playlist']

connection = mysql.connector.connect(host='localhost',
                                        database='spotify',
                                        user='admin',
                                        password='admin')
cursor = connection.cursor()
print("Connected to mysql")