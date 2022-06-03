from urllib import response
import boto3
import sqlite3
import argparse
import requests
import sys
import os
import json
import psycopg2
import pandas as pd

class color:
   PURPLE = '\033[1;35;48m'
   CYAN = '\033[1;36;48m'
   BOLD = '\033[1;37;48m'
   BLUE = '\033[1;34;48m'
   GREEN = '\033[1;32;48m'
   YELLOW = '\033[1;33;48m'
   RED = '\033[1;31;48m'
   UNDERLINE = '\033[4;37;48m'
   END = '\033[1;37;0m'


ACVDB_HOST = "acvdb.chhjlyz9fpln.us-east-1.rds.amazonaws.com"
ACVDB_USER = "svcdbadmin"
ACVDB_PASS = os.environ["PGPASSWORD"]
ACVDB_DATABASE = "channelsvars"

PATH_PATTERNS=[
    "/ads/*",
    "/*.m3u8",
    "/*.ts",
    "/*.key",
    "/*.vtt",
    "/*.webvtt",
]

CORS_HEADER_KEYS=[
    'access-control-allow-origin',
    'access-control-allow-methods',
    'access-control-allow-headers',
    'access-control-expose-headers',
]

def get_headers(channel_url):
    headers = {"Origin":"www.example.com"}
    r = requests.get(channel_url, headers=headers)
    cors_headers = dict((k, r.headers[k]) for k in CORS_HEADER_KEYS if k in r.headers)
    return cors_headers

def validate_cors(channel_url):
    print(f"{color.YELLOW} == Validating CORS headers for {color.CYAN}{channel_url}{color.END}")
    # channel_url = get_channel_url(distro_id)
    print(f"Channel URL: {color.CYAN}{channel_url}{color.END}")
    try:
        response_headers = get_headers(channel_url)
        # print(json.dumps(dict(response_headers), indent=2))
        if "access-control-allow-origin" not in response_headers.keys():
            return False
        elif response_headers['access-control-allow-origin'] != "*":
            return False
        elif response_headers['access-control-allow-methods'] != "*":
            return False
        elif response_headers['access-control-allow-headers'] != "range":
            return False
        elif response_headers['access-control-expose-headers'] != "content-length,content-range":
            return False
        else:
            return True
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
        


def get_channel_url(distro_id):
    try:
        acvdb_conn = psycopg2.connect(
            f"dbname='{ACVDB_DATABASE}' user='{ACVDB_USER}' host='{ACVDB_HOST}' password='{ACVDB_PASS}'"
        )
    except Exception as error:
        print(f"Failed to connect to ACVDB due to the following error: {error}")
        sys.exit(1)
    
    dataframe = pd.read_sql_query(
        sql=(
            f"select channel_url from hlsrebroadcast where cdn_dns = '{distro_id}';"
        ),
        con=acvdb_conn,
    )
    if dataframe.empty:
        # Try looking in hlsplayout instead
            dataframe = pd.read_sql_query(
        sql=(
            f"select channel_url from hlsplayout where cdn_dns = '{distro_id}';"
        ),
        con=acvdb_conn,
        )
    if dataframe.empty:
        acvdb_conn.close()
        return None
    #    print(f"{color.RED}Unable to find {color.PURPLE}{distro_id}{color.RED} in ACVDB{color.END}")
    #    sys.exit(1)
    
    channel_url = dataframe['channel_url'][0]
    acvdb_conn.close()
    return channel_url

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--old",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--new",
        type=str,
        required=True,
    )
    return parser.parse_args()

def main():
    args = parse_args()
    client = boto3.client('cloudfront')
    # Setup DB connection
    try:
        dbconn = sqlite3.connect('cors.db')
    except:
        print("Error connecting to database")
    cursor = dbconn.cursor()
        # Check if one of the tables already exists 
    cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='distros'")
    if cursor.fetchone()[0]!=1:
        sys.exit(f"{color.RED}The distros table does not exist! Check cors.db.{color.END}")

    # Which *old* Lambda ARN are we updating?
    old_arn = args.old
    print(f"{color.BLUE}Old Lambda ARN: {old_arn}{color.END}")
    # Which *new* Lambda ARN are we using?
    new_arn = args.new
    print(f"{color.GREEN}New Lambda ARN: {new_arn}{color.END}")
    # Get the rowID of the old ARN
    cursor.execute(f"SELECT rowid FROM lambdas WHERE arn = '{old_arn}'")
    old_arn_id = cursor.fetchone()[0]
    if old_arn_id == None:
        sys.exit(f"{color.RED}Old ARN: {old_arn} was not found in the database!{color.END}")
    # Get a list of distribution IDs using that Lambda ARN
    cursor.execute(f"SELECT id FROM distros WHERE lambda = {old_arn_id}")
    distros_to_update = cursor.fetchall()
        # Print a count of the distributions for audit purposes
    num_distros = len(distros_to_update)
    print(f"Number of distributions using this ARN: {num_distros}")
    distros_updated = 0
    distros_skipped = []
    # Foreach distro get current config
    for distro in distros_to_update:
        # Check if this distro has a channel URL
        # If it doesn't, this is probably a DEV channel or on Highwinds so we skip it
        channel_url = get_channel_url(distro[0])
        if channel_url is None:
            print(f"{color.RED}Didn't find {color.PURPLE}{distro[0]}{color.RED} in ACVDB. Skipping...{color.END}")
            distros_skipped.append(distro[0])
            continue
        # Get the current config and Etag
        print(f"Getting config for {color.PURPLE}{distro[0]}{color.END}")
        old_config = client.get_distribution_config(Id=distro[0])
        # print(json.dumps(old_config, indent=4, sort_keys=False))
        etag = old_config["ETag"]
        print(f" {color.YELLOW}== Updating config with new ARN{color.END}")
        new_config = old_config['DistributionConfig']
        original_headers = get_headers(channel_url)
        print(f"{color.BLUE}Original headers:{color.END}")
        print(json.dumps(dict(original_headers), indent=2))

        # Update LambdaFunctionARN to the new ARN
        for extension in PATH_PATTERNS:
            print(f"{color.PURPLE}{extension}{color.END}")
            for pattern in new_config["CacheBehaviors"]["Items"]:
                if pattern["PathPattern"] == extension:
                    for lambda_association in pattern["LambdaFunctionAssociations"]["Items"]:
                        if lambda_association["LambdaFunctionARN"] == old_arn:
                            print(f"{color.GREEN}Old Lambda ARN found: {color.CYAN}{lambda_association['LambdaFunctionARN']}{color.END}")
                            print(f"{color.YELLOW} == Replacing with new Lambda ARN: {color.CYAN}{new_arn}{color.END}")
                            lambda_association['LambdaFunctionARN'] = new_arn
                            # print(f"     {lambda_association['LambdaFunctionARN']}")
                            # print(json.dumps(new_config, indent=4, sort_keys=False))
                        else:
                            print(f"{color.BLUE}Lambda ARN found that did not match: {color.CYAN}{lambda_association['LambdaFunctionARN']}{color.END}")


        # Validate we get the correct CORS headers back
        valid_cors = validate_cors(channel_url)
        # If not, push the old ARN back in and revalidate, then exit with error
        if not valid_cors:
            print(f" {color.RED}         ~! UNABLE TO VALIDATE CORS HEADERS AFTER UPDATE !~{color.END}")
            print(f"  {color.YELLOW}                == Rolling back to old ARN: {color.CYAN}{old_arn}...{color.END}")
            for extension in PATH_PATTERNS:
                print(f"{color.PURPLE}{extension}{color.END}")
                for pattern in new_config["CacheBehaviors"]["Items"]:
                    if pattern["PathPattern"] == extension:
                        for lambda_association in pattern["LambdaFunctionAssociations"]["Items"]:
                            if lambda_association["LambdaFunctionARN"] == new_arn:
                                print(f"{color.GREEN}New Lambda ARN found: {color.CYAN}{lambda_association['LambdaFunctionARN']}{color.END}")
                                print(f"{color.YELLOW} == Restoring Old Lambda ARN: {color.CYAN}{old_arn}{color.END}")
                                lambda_association['LambdaFunctionARN'] = old_arn
                                # print(f"     {lambda_association['LambdaFunctionARN']}")
                                # print(json.dumps(new_config, indent=4, sort_keys=False))
                            else:
                                print(f"{color.BLUE}Lambda ARN found that did not match: {color.CYAN}{lambda_association['LambdaFunctionARN']}{color.END}")
            rollback_headers = get_headers(channel_url)
            print(f"{color.BLUE}Headers after rollback:{color.END}")
            print(json.dumps(dict(rollback_headers), indent=2))
            if rollback_headers == original_headers: 
                print(f"{color.RED}Unable to validate CORS headers on {color.PURPLE}{distro[0]}!")
                print(f"{color.RED}I have rolled back to the old ARN {color.CYAN}{old_arn}{color.RED} and confirmed the original headers are restored.{color.END}")
                print(f"Distributions Updated: {distros_updated}")
                num_skipped = len(distros_skipped)
                print(f"Distributions skipped: {num_skipped}")
                print(distros_skipped)
                sys.exit()
            else:
                print(f"{color.RED}Unable to validate CORS headers on {color.PURPLE}{distro[0]}!")
                print(f"{color.RED} I attempted to roll back but still cannot validate the headers!{color.END}")
                print(f"Distributions Updated: {distros_updated}")
                num_skipped = len(distros_skipped)
                print(f"Distributions skipped: {num_skipped}")
                print(distros_skipped)
                sys.exit()
            
        distros_updated += 1
    print(f"Distributions Updated: {distros_updated}")
    num_skipped = len(distros_skipped)
    print(f"Distributions skipped: {num_skipped}")
    print(distros_skipped)


    # Clean up
    dbconn.close()

if __name__ == "__main__":
    main()