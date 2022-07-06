import boto3
import sqlite3
import argparse
import requests
import sys
import os
import json
import psycopg2
import pandas as pd
from botocore.config import Config

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

AWS_CREDENTIALS = {
    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
}

DEFAULT_WURL_AWS_ACCOUNT = "root"
WURL_AWS_ACCOUNTS = {
    "root": "",
    "sandbox": "arn:aws:iam::709097557611:role/wurl-sandboxSTSRole",
}

def update_config(config, distro_id, etag, client):
    print(f" {color.YELLOW}== Updating Distribution Config == {color.END}")
    response = client.update_distribution(DistributionConfig=config, Id=distro_id, IfMatch=etag)
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(f"{color.GREEN}~ SUCCESS ~{color.END}")
    else:
        metadata = response["ResponseMetadata"]
        print(f"{color.RED}Failed to update configuration on {color.PURPLE}{distro_id}{color.END}")
        print(json.dumps(metadata, indent=2))

def get_headers(channel_url):
    headers = {"Origin":"www.example.com"}
    try:
        r = requests.get(channel_url, headers=headers)
    except requests.exceptions.RequestException as e:
        print(f"{color.RED}Unable to perform HTTP request to {color.CYAN}{channel_url}{color.END}")
        print(f"{color.RED}HTTP error was: {color.BOLD}{e}{color.END}")
        return None
    cors_headers = dict((k, r.headers[k].lower()) for k in CORS_HEADER_KEYS if k in r.headers)
    return cors_headers

def validate_cors(channel_url):
    print(f"{color.YELLOW} == Validating CORS headers for {color.CYAN}{channel_url}{color.END}")
    # channel_url = get_channel_url(distro_id)
    print(f"Channel URL: {color.CYAN}{channel_url}{color.END}")
    try:
        response_headers = get_headers(channel_url)
        if response_headers == None:
            return False
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
            print(f"{color.GREEN}Successfully validated headers for {color.CYAN}{channel_url}{color.END}")
            print(f"{color.BLUE}New headers:{color.END}")
            print(json.dumps(dict(response_headers), indent=2))
            return True
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
        


def get_channel_url(distro_id, profile, client):
    # Are we working in the sandbox account? If not, use ACVDB to lookup the channel URL
    if profile != 'sandbox':
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
    elif profile == 'sandbox':
        # Lookup the channel_url in the sandbox table
        print(f"{color.YELLOW}I'm in sandbox mode{color.END}")
        config = client.get_distribution_config(Id=distro_id)
        channel_url = 'https://' + config['DistributionConfig']['Aliases']['Items'][0] + '/playlist.m3u8'
        # channel_url = 'https://' + distro_id.lower() + '.cloudfront.net'
        return channel_url

    else:
        # Something's wrong and we can't determine the profile
        print(f"{color.RED}Cannot determine which profile I'm in. It's currently set to {color.PURPLE}{profile}{color.END}")
        return None

    channel_url = dataframe['channel_url'][0]
    acvdb_conn.close()
    return channel_url

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--old",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--new",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--validate",
        action='store_true',
    )
    parser.add_argument(
        "--profile",
        type=str,
        choices=WURL_AWS_ACCOUNTS.keys(),
        default=DEFAULT_WURL_AWS_ACCOUNT,
        required=True,
    )
    return parser.parse_args()

def get_aws_assumed_role_credentials(account_name, credentials):
    sts_credentials = {}
    role = WURL_AWS_ACCOUNTS.get(account_name)
    if role is not None:
        client = boto3.client(
            "sts",
            aws_access_key_id=credentials["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=credentials["AWS_SECRET_ACCESS_KEY"],
        )
        assumed_role = client.assume_role(
            RoleArn=role, RoleSessionName=f"Assumed_{account_name}_Session"
        )
        sts_credentials = assumed_role["Credentials"]
    return sts_credentials

def main():
    args = parse_args()

    if args.profile != DEFAULT_WURL_AWS_ACCOUNT:
        sts_credentials = get_aws_assumed_role_credentials(args.profile, AWS_CREDENTIALS)
        AWS_CREDENTIALS["AWS_ACCESS_KEY_ID"] = sts_credentials["AccessKeyId"]
        AWS_CREDENTIALS["AWS_SECRET_ACCESS_KEY"] = sts_credentials["SecretAccessKey"]
        AWS_CREDENTIALS["AWS_SESSION_TOKEN"] = sts_credentials["SessionToken"]
    client = boto3.client(
        "cloudfront",
        aws_access_key_id=AWS_CREDENTIALS["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=AWS_CREDENTIALS["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=AWS_CREDENTIALS.get("AWS_SESSION_TOKEN", None),
        region_name="us-east-1",
        config=Config(retries={"max_attempts": 10, "mode": "adaptive"}),
    )

    # Setup DB connection
    try:
        dbconn = sqlite3.connect('batch1.db')
    except:
        print("Error connecting to database")
    cursor = dbconn.cursor()
        # Check if one of the tables already exists 
    cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='distros'")
    if cursor.fetchone()[0]!=1:
        sys.exit(f"{color.RED}The distros table does not exist! Check cors.db.{color.END}")
    distros_to_work_on = []
    # Are we validating or updating?
    if not args.validate:
        print(f"{color.YELLOW} == Running in {color.RED}UPDATE{color.YELLOW} mode!{color.END}")
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
        cursor.execute(f"SELECT DISTINCT id FROM distros WHERE lambda = {old_arn_id}")
        distros_to_work_on = cursor.fetchall()
        # Print a count of the distributions for audit purposes
        print(f"Number of distributions using this ARN: {len(distros_to_work_on)}")
    else:
        print(f"{color.PURPLE}== Running in {color.GREEN}VALIDATE{color.PURPLE} mode.{color.END}")
        cursor.execute("SELECT DISTINCT id FROM distros")
        distros_to_work_on = cursor.fetchall()

    distros_updated = []
    distros_skipped = []
    distros_rolled_back = []
    # Foreach distro get current config
    for distro in distros_to_work_on:
        # Check if this distro has a channel URL
        # If it doesn't, this is probably a DEV channel or on Highwinds so we skip it
        channel_url = get_channel_url(distro[0], args.profile, client)
        if channel_url is None:
            print(f"{color.RED}Didn't find {color.PURPLE}{distro[0]}{color.RED} in ACVDB. Skipping...{color.END}")
            distros_skipped.append(distro[0])
            continue
        # If running in validation mode, get the headers from the channel_url and compare to see if they're what we want
        if args.validate:
            print(f"{color.CYAN}Distribution ID: {color.PURPLE}{distro[0]}{color.END}")
            if validate_cors(channel_url):
                print(f"{color.GREEN}Successfully validated CORS headers for {color.PURPLE}{distro[0]}{color.END}")
                distros_updated.append(distro[0])
                continue
            else:
                print(f"{color.RED}Unable to validate CORS headers for {color.PURPLE}{distro[0]}{color.END}")
                # headers = get_headers(channel_url)
                # print(json.dumps(headers, indent=2))
                distros_skipped.append(distro[0])
                continue
        # If not running in validate mode, we need to actually do stuff
        elif not args.validate:
            # Get the current config and Etag
            print(f"Getting config for {color.PURPLE}{distro[0]}{color.END}")
            old_config = client.get_distribution_config(Id=distro[0])
            # print(json.dumps(old_config, indent=4, sort_keys=False))
            etag = old_config["ETag"]
            print(f" {color.YELLOW}== Updating config with new ARN{color.END}")
            new_config = old_config['DistributionConfig']
            original_headers = get_headers(channel_url)
            if original_headers is None:
                print(f"{color.RED}Unable to get headers. Skipping this distro...{color.END}")
                distros_skipped.append(distro[0])
                continue

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
                                print(f"{color.BLUE}Lambda ARN found that did not match: {color.END}{lambda_association['LambdaFunctionARN']}")
            # Push the new config to Cloudfront
            update_config(new_config, distro[0], etag, client)

            # Validate we get the correct CORS headers back
            valid_cors = validate_cors(channel_url)
            # If not, push the old ARN back in and revalidate, then exit with error
            if not valid_cors:
                print(f" {color.RED}         ~! UNABLE TO VALIDATE CORS HEADERS AFTER UPDATE !~{color.END}")
                print(f"  {color.YELLOW}                == Rolling back to old ARN: {color.CYAN}{old_arn}...{color.END}")
                rollback_config = new_config
                for extension in PATH_PATTERNS:
                    print(f"{color.PURPLE}{extension}{color.END}")
                    for pattern in rollback_config["CacheBehaviors"]["Items"]:
                        if pattern["PathPattern"] == extension:
                            for lambda_association in pattern["LambdaFunctionAssociations"]["Items"]:
                                if lambda_association["LambdaFunctionARN"] == new_arn:
                                    print(f"{color.GREEN}New Lambda ARN found: {color.CYAN}{lambda_association['LambdaFunctionARN']}{color.END}")
                                    print(f"{color.YELLOW} == Restoring Old Lambda ARN: {color.CYAN}{old_arn}{color.END}")
                                    lambda_association['LambdaFunctionARN'] = old_arn
                                    # print(f"     {lambda_association['LambdaFunctionARN']}")
                                    # print(json.dumps(new_config, indent=4, sort_keys=False))
                                else:
                                    print(f"{color.BLUE}Lambda ARN found that did not match: {color.END}{lambda_association['LambdaFunctionARN']}")
                # Push the rollback config to AWS
                update_config(rollback_config, distro[0], etag, client)

                # Validate we get the original headers after rolling back
                rollback_headers = get_headers(channel_url)
                print(f"{color.BLUE}Headers after rollback:{color.END}")
                print(json.dumps(dict(rollback_headers), indent=2))
                if rollback_headers == original_headers: 
                    print(f"{color.RED}Unable to validate CORS headers on {color.PURPLE}{distro[0]}!")
                    print(f"{color.RED}I have rolled back to the old ARN {color.CYAN}{old_arn}{color.RED} and confirmed the original headers are restored.{color.END}")
                    distros_rolled_back.append(distro[0])
                    continue
                else:
                    print(f"{color.RED}Unable to validate CORS headers on {color.PURPLE}{distro[0]}!")
                    print(f"{color.RED} I attempted to roll back but still cannot validate the headers!{color.END}")
                    print(f"Distributions Updated: {distros_updated}")
                    num_skipped = len(distros_skipped)
                    print(f"Distributions skipped: {num_skipped}")
                    print(distros_skipped)
                    sys.exit()
            
        distros_updated.append(distro[0])
    print(f"Distributions Updated: {len(distros_updated)}")
    print(f"Distributions rolled back: {len(distros_rolled_back)}")
    print(f"Distributions skipped: {len(distros_skipped)}")
    print(distros_skipped)


    # Clean up
    dbconn.close()

if __name__ == "__main__":
    main()