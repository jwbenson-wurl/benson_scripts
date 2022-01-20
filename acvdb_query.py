import os
import psycopg2
import pandas as pd

ACVDB_HOST = "acvdb.chhjlyz9fpln.us-east-1.rds.amazonaws.com"
ACVDB_USER = "svcdbadmin"
ACVDB_PASS = os.environ["PGPASSWORD"]
ACVDB_DATABASE = "channelsvars"

# Setup DB connection
try:
    acvdb_conn = psycopg2.connect(
        f"dbname='{ACVDB_DATABASE}' user='{ACVDB_USER}' host='{ACVDB_HOST}' password='{ACVDB_PASS}'"
    )
except Exception as error:
    print(f"Failed to connect to ACVDB due to the following error: {error}")


def getDistributionId(acvdb_conn, channel_key):
    # query the DB to get the distroID
    query = f"SELECT DISTINCT hlsplayout.cdn_dns, hlsplayout.bootstrap_url FROM hlsplayout,hlsrebroadcast WHERE hlsplayout.channel_key = '{channel_key}' OR hlsrebroadcast.channel_key = '{channel_key}';"
    df = pd.read_sql_query(query, acvdb_conn)
    if df.empty:
        return False
    else:
        cdn_dns = df["cdn_dns"][0]
        return cdn_dns
      
