# A script to update the version of the cloudfront-cors-append lambda function
# used by CloudFront distributions
# Usage:
# cors_update.py --mode [noop|single|all] [--id <distribution ID>]

import os
from pickle import TRUE
import boto3
import argparse
import json
import sqlite3
import time
from botocore.config import Config
import sys
import pandas as pd
import psycopg2

AWS_CREDENTIALS = {
    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
}

DEFAULT_WURL_AWS_ACCOUNT = "root"
WURL_AWS_ACCOUNTS = {
    "root": "",
    "sandbox": "arn:aws:iam::709097557611:role/wurl-sandboxSTSRole",
}

OPMODE = [
    "noop",
    "single",
    "all",
]

PATH_PATTERNS=[
    "/ads/*"
    "/*.m3u8",
    "/*.ts",
    "/*.key",
    "/*.vtt",
    "/*.webvtt",
]

ACVDB_HOST = "acvdb.chhjlyz9fpln.us-east-1.rds.amazonaws.com"
ACVDB_USER = "svcdbadmin"
ACVDB_PASS = os.environ["PGPASSWORD"]
ACVDB_DATABASE = "channelsvars"

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        type=str,
        choices=OPMODE,
        default="noop",
        required=True,
    )
    parser.add_argument(
        "--id",
        type=str,
        required=False,
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

    try:
        acvdb_conn = psycopg2.connect(
            f"dbname='{ACVDB_DATABASE}' user='{ACVDB_USER}' host='{ACVDB_HOST}' password='{ACVDB_PASS}'"
        )
        acvdb_cur = acvdb_conn.cursor()
    except Exception as error:
        print(f"Failed to connect to ACVDB due to the following error: {error}")
        sys.exit(1)

    dbconn = sqlite3.connect('cors.db')
    cursor = dbconn.cursor()
    # Check if on of the tables already exists so we don't overwrite them
    cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='distros'")
    distros_exists = False
    if cursor.fetchone()[0]==1:
        distros_exists = True

    if not distros_exists:
        cursor.execute("CREATE TABLE distros (id varchar(18) NOT NULL UNIQUE, lambda int, channel_key varchar(255))")
        cursor.execute("CREATE TABLE lambdas (arn varchar(255) NOT NULL UNIQUE)")
    distros = []
    count_of_distros = 0
    distros_to_update = 0
    # If we're in single channel mode, the distros list will contain only one object
    if args.mode == "single":
        distros.append(args.id)
    # If we're in noop or all mode we need to get the list of distros that are using the current
    # CORS lambda
    elif (args.mode == "noop") or (args.mode == "all"):
        # Get list of distros
        next_marker = None
        more_pages = True
        while more_pages:
            if next_marker is None:
                all_distros = client.list_distributions()
            else: 
               all_distros = client.list_distributions(Marker=next_marker)
            for distribution in all_distros["DistributionList"]["Items"]:
               distros.append(distribution["Id"])
            next_marker = all_distros['DistributionList'].get("NextMarker", None)
            if next_marker is None:
                more_pages = False
            time.sleep(0.5)


    for distro in distros:
        print(f"Getting config for {distro}")
        distro_config = client.get_distribution_config(Id=distro)
        site_name = distro_config["DistributionConfig"]["CallerReference"]
        print(f"Site Name: {site_name}")
        # Does the m3u8 path have a LambdaFunctionAssociation?
        if distro_config["DistributionConfig"]["CacheBehaviors"]["Quantity"] == 0:
            print(f"{distro} has no CacheBehaviors defined. Skipping.")
            continue
        for path in distro_config["DistributionConfig"]["CacheBehaviors"]["Items"]:
            if path["PathPattern"] in PATH_PATTERNS:
                print(json.dumps(path, indent=4, sort_keys=False))
                if path["LambdaFunctionAssociations"]["Quantity"] != 0:
                    lambda_arn = path["LambdaFunctionAssociations"]["Items"][0]["LambdaFunctionARN"]
                    print(lambda_arn)
                    # INSERT IGNORE the lambda ARN into the lambdas table and get the key back
                    cursor.execute(f"INSERT OR IGNORE INTO lambdas (arn) VALUES ('{lambda_arn}')")
                    cursor.execute(f"SELECT rowid FROM lambdas WHERE arn = '{lambda_arn}'")
                    lambda_key = cursor.fetchone()[0]
                    print(lambda_key)
                    channel_key = ''
                    dataframe = pd.read_sql_query(
                        sql=(
                            f"select channel_key from hlsrebroadcast where cdn_dns = '{distro}';"
                        ),
                        con=acvdb_conn,
                    )
                    if dataframe.empty:
                        print(f"No channel key found for {distro}")
                    else:
                        channel_key = dataframe['channel_key'][0]
                    # INSERT IGNORE distro ID and key from the lambda table into the distros table
                    cursor.execute(f"INSERT OR IGNORE INTO distros (id,lambda,channel_key) VALUES ('{distro}',{lambda_key},'{channel_key}')")
                    # distros_to_update += 1
        count_of_distros += 1
        
    print(f"Distros scanned: {count_of_distros}")
    cursor.execute("SELECT count(*) FROM distros")
    row_count = cursor.fetchone()[0]
    print(f"Number of distros to update: {row_count}")
    print(f"Number of rows inserted into Distros: {row_count}")
    cursor.execute("SELECT rowid, arn, (SELECT count(*) from distros WHERE lambda = lambdas.rowid GROUP BY lambda) num FROM lambdas WHERE arn LIKE '%cloudfront-cors-append%'")
    lambda_table = cursor.fetchall()
    print(*lambda_table, sep='\n')

    dbconn.commit()
    dbconn.close()


if __name__ == "__main__":
    main()