import pandas as pd
import mysql.connector

# Extract connection parameters
db_config = {
    'user': 'avnadmin',
    'password': 'AVNS_4T0M1r4SdTxtoZAMUPL',
    'host': 'mysql-c314d3b-reveliolab-assignment1.e.aivencloud.com',
    'port': 24878,
    'ssl_disabled': False  # Enforces SSL connection
}

try:
    # Establish a connection
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
except mysql.connector.Error as err:
    print(f"Error: {err}")

with open("../db_setup.sql", "r", encoding="utf-8") as file:
    sql_script = file.read()
try:
    for statement in sql_script.split(";\n"):  # Split only at full statement endings
        statement = statement.strip()
        if statement:
            try:
                cursor.execute(statement)
            except mysql.connector.Error as err:
                print(f"Skipping error: {err}")
except Exception as e:
    print(f"Critical Error: {e}")

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()
