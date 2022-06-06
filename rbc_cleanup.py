import psycopg2
import pandas as pd
import os
import sys

ACVDB_HOST = "acvdb.chhjlyz9fpln.us-east-1.rds.amazonaws.com"
ACVDB_USER = "svcdbadmin"
ACVDB_PASS = os.environ["PGPASSWORD"]
ACVDB_DATABASE = "channelsvars"

CHANNEL_LIST_FILE = sys.argv[1]

def get_acvdb_config(acvdb_conn, channel_key):

    dataframe = pd.read_sql_query(
        sql=(
            f"select channel_key, shortname, channel_slug, channel_url, node, manifest_port, origin_manifest, origin_segments, rebroadcast_technology from hlsrebroadcast "
            f"where channel_key = '{channel_key}';"
        ),
        con=acvdb_conn,
    )
    return dataframe

def main():
    nl = '\n'
    this_batch = pd.DataFrame()
    try:
        acvdb_conn = psycopg2.connect(
            f"dbname='{ACVDB_DATABASE}' user='{ACVDB_USER}' host='{ACVDB_HOST}' password='{ACVDB_PASS}'"
        )
    except Exception as error:
        print(f"Failed to connect to ACVDB due to the following error: {error}")
        sys.exit(1)
    num_channels = 0
    with open(CHANNEL_LIST_FILE) as fp:
        channel_keys = fp.readlines()

        for index, channel_key in enumerate(channel_keys):
            channel_key = channel_key.rstrip()
            result = get_acvdb_config(acvdb_conn, channel_key)
            this_batch = this_batch.append(result)
            # channel_key, shortname, channel_slug, channel_url, node, manifest_port, origin_manifest, origin_segments, rebroadcast_technology = result.values.tolist()[0]
            num_channels += 1
    pd.set_option('display.max_colwidth', None)        
    print(this_batch[['channel_key', 'shortname', 'node', 'rebroadcast_technology', 'origin_segments']])
    print(f"Channels Found: {num_channels}")
    acvdb_conn.close()

if __name__ == "__main__":
    main()