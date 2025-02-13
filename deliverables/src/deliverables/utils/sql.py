import mysql.connector
import pandas as pd

def db_conn(warehouse=None):
    connection = mysql.connector.connect(
        user="avnadmin",
        password="AVNS_4T0M1r4SdTxtoZAMUPL",
        host="mysql-c314d3b-reveliolab-assignment1.e.aivencloud.com",  # For instances that run on the same machine as the client
        port=24878,  # Use port 4566 for non-SSL, 443 for SSL connections
        ssl_disabled=False
    )
    return connection


def execute(sql_str):
    statements = sql_str.strip().split(';')
    conn = db_conn()
    cur = conn.cursor()
    for s in statements:
        cur.execute(s)
        conn.commit()


def load_df(sql):
    conn = db_conn()
    return pd.read_sql(sql, conn)
