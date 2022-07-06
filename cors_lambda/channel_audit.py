import pandas as pd
import psycopg2
import sys
import os
import sqlite3

ACVDB_HOST = "acvdb.chhjlyz9fpln.us-east-1.rds.amazonaws.com"
ACVDB_USER = "svcdbadmin"
ACVDB_PASS = os.environ["PGPASSWORD"]
ACVDB_DATABASE = "channelsvars"

try:
    acvdb_conn = psycopg2.connect(
        f"dbname='{ACVDB_DATABASE}' user='{ACVDB_USER}' host='{ACVDB_HOST}' password='{ACVDB_PASS}'"
    )
    cur = acvdb_conn.cursor()
except Exception as error:
    print(f"Failed to connect to ACVDB due to the following error: {error}")
    sys.exit(1)

try:
    dbconn = sqlite3.connect('cors.db')
except:
    print("Error connecting to database")
cursor = dbconn.cursor()
    # Check if one of the tables already exists 
cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='distros'")
if cursor.fetchone()[0]!=1:
    sys.exit(f"The distros table does not exist! Check cors.db.")

cursor.execute("SELECT id FROM distros")
distros_to_work_on = cursor.fetchall()

file=open('channel_keys.txt', 'w')

for distro in distros_to_work_on:
    distro_id = distro[0]
    dataframe = pd.read_sql_query(
        sql=(
            f"select channel_key from hlsrebroadcast where cdn_dns = '{distro_id}';"
        ),
        con=acvdb_conn,
    )
    if dataframe.empty:
        # Try looking in hlsplayout instead
            dataframe = pd.read_sql_query(
        sql=(
            f"select channel_key from hlsplayout where cdn_dns = '{distro_id}';"
        ),
        con=acvdb_conn,
        )
    if dataframe.empty:
        print(f"No results found for {distro_id}")
        continue
    channel_key = dataframe['channel_key'][0]
    print(f"Found channel key {channel_key} for distribution ID {distro_id}")
    file.writelines(channel_key+'\n')

      
file.close()
acvdb_conn.close()
        