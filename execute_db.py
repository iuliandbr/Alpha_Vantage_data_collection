import psycopg2 as pg
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def connectDB_main():
    con = pg.connect(user = 'postgres', host = 'localhost', password = 'admin')
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = con.cursor()
    return con, cursor

def getDBs():
    con, cursor = connectDB_main()

    # Create table statement
    sql_dbs = 'SELECT datname FROM pg_catalog.pg_database'

    cursor.execute(sql_dbs)
    DBs = []
    for DB in cursor.fetchall():
        #print(DB)
        DBs.append(DB[0])
    return DBs

def connectDatabase(db_name):
    con = pg.connect(dbname=db_name, user='postgres', host='localhost', password='admin')

    return con


def getTables(db_name):
    # Connect to DataBase
    con = connectDatabase(db_name)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = con.cursor()

    cursor.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")

    tables = []
    for table in cursor.fetchall():
        print(table)
        tables.append(table[0])

    return tables

def build_db(db_name):
    con, cursor = connectDB_main()
    name_Database = db_name

    # Create table statement
    sqlCreateDatabase = "create database " + name_Database + ";"
    cursor.execute(sqlCreateDatabase)
    
def build_table(db_name,tickers):
    # Connect to DataBase
    con = connectDatabase(db_name)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = con.cursor()

    for ticker in tickers:
        trades_table_query = """
             CREATE TABLE """ +'raw_'+ticker+"""(
             date varchar(20) NULL,
             open int NULL,
             high int NULL,
             low int NULL,
             close int NULL,
             volume int NULL
             ) """
        cursor.execute(trades_table_query)

    con.commit()
    
def push_data(db_name,data):
    # Connect to DataBase
    con = connectDatabase(db_name)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = con.cursor()

    insert_query = """ INSERT INTO raw_aapl (date, open, high, low, close, volume) 
                        VALUES (%s,%s,%s,%s,%s,%s) """

    data_to_db = ['data', 1, 100, 1, 1,1]

    cursor.execute(insert_query, data_to_db)
    con.commit()