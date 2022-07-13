import yaml
import psycopg2
import os
import sys
import pandas as pd
from pathlib import Path

RBC_CONFIG_ROOT = '/Users/josh/wurl-git/ansible-rebroadcast-deploy/rebroadcasts/'

ACVDB_HOST = "acvdb.chhjlyz9fpln.us-east-1.rds.amazonaws.com"
ACVDB_USER = "svcdbadmin"
ACVDB_PASS = os.environ["PGPASSWORD"]
ACVDB_DATABASE = "channelsvars"

def get_acvdb_origin(shortname, acvdb_cur):
    # Return the origin_segments value from ACVDB
    print(f"Getting ACVDB configured origin for {shortname}")
    acvdb_cur.execute(f"SELECT origin_segments FROM hlsrebroadcast WHERE shortname = '{shortname}';")
    db_origin = acvdb_cur.fetchone()
    if db_origin is None:
        return db_origin
    else:
        db_origin = db_origin[0]
        return db_origin


def main():

    # Setup ACVDB connection
    try:
        acvdb_conn = psycopg2.connect(
            f"dbname='{ACVDB_DATABASE}' user='{ACVDB_USER}' host='{ACVDB_HOST}' password='{ACVDB_PASS}'"
        )
        acvdb_cur = acvdb_conn.cursor()
    except Exception as error:
        print(f"Failed to connect to ACVDB due to the following error: {error}")
        sys.exit(1)

    # Setup a dataframe to store results
    table = pd.DataFrame(columns=['shortname', 'yaml_origin', 'db_origin', 'match_state'])

    # Scan RBC_CONFIG_ROOT to get a list of yaml files
    yaml_files = list(Path(RBC_CONFIG_ROOT).rglob("*.yml"))
    print(f"Found {len(yaml_files)} configuration files to scan...")

    # Foreach yaml file, get the origin_segments value then compare to the ACVDB value
    for file in yaml_files:
        print(f"Parsing {file}")
        with open(file, 'r') as config_file:
            match_state = False
            config = yaml.safe_load(config_file)
            shortname = config['config']['shortname']
            print(f"      Shortname: {shortname}")
            yaml_origin = config['config']['segment_url']
            print(f"      Yaml Origin: {yaml_origin}")
            db_origin = get_acvdb_origin(shortname, acvdb_cur)
            print(f"      ACVDB Origin: {db_origin}")
            if yaml_origin == db_origin:
                match_state = True
            table = table.append({'shortname':shortname, 'yaml_origin': yaml_origin, 'db_origin': db_origin, 'match_state': match_state}, ignore_index=True)


    print("Saving results to rbc_audit.csv")
    table.to_csv('rbc_audit.csv')

if __name__ == "__main__":
    main()