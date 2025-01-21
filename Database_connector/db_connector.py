import datetime
from random import random

import psycopg2
import random
from datetime import date, timedelta
hostname = 'localhost'
username ='postgres'
pwd = 'postgres'
port_id = 5433
DATABASE_NAME = 'ETL_Conn'

class Database:
    def connect_postgres(self,dbname):

        conn = None
        curr = None
        try:
            conn = psycopg2.connect(
                host = hostname,
                dbname = dbname,
                user = username,
                password = pwd,
                port = port_id
            )
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            print("Connection successfull")
        except Exception as error:
            print(error)
        return conn
    
    def create_database(self,conn):
        self.conn= self.connect_postgres(dbname='postgres')
        if conn is None:
            print("connection is not successful")
            return 
        
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DATABASE_NAME}'")
        self.exists = self.cursor.fetchone()

        if not self.exists:
            self.cursor.execute(f"create database {DATABASE_NAME}")
        else:
            print(f"Database {DATABASE_NAME} already exists")
    
        self.conn.commit()
        self.conn.close()
        self.cursor.close()
    

if __name__ == '__main__':
    dbobj = Database()
    dbobj.connect_postgres(dbname='postgres')
    dbobj.create_database(DATABASE_NAME)
    with dbobj.connect_postgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        print('Done')




