import pandas as pd
import boto3
from botocore.config import Config
import sqlite3
import json
import os
import argparse


PATH_PATTERNS=[
    "/ads/*",
    "/*.m3u8",
    "/*.ts",
    "/*.key",
    "/*.vtt",
    "/*.webvtt",
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


def parse_args():
    parser = argparse.ArgumentParser()
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



def get_lambdas(cf_client, distro_id, cursor):
    num_lambdas = 0
    print(f"Getting config for {distro_id}")
    try:
        distro_config = cf_client.get_distribution_config(Id=distro_id)
    except:
        print(f"Unable to get configuration for {distro_id}, skipping")
        return None
    site_name = distro_config["DistributionConfig"]["CallerReference"]
    print(f"Site Name: {site_name}")
    # Does the path have a LambdaFunctionAssociation?
    if distro_config["DistributionConfig"]["CacheBehaviors"]["Quantity"] == 0:
        print(f"{distro_id} has no CacheBehaviors defined. Skipping.")
        return None
    for path in distro_config["DistributionConfig"]["CacheBehaviors"]["Items"]:
        if path["PathPattern"] in PATH_PATTERNS:
            pattern = path["PathPattern"]
            print(json.dumps(path, indent=4, sort_keys=False))
            if path["LambdaFunctionAssociations"]["Quantity"] != 0:
                num_lfas = path["LambdaFunctionAssociations"]["Quantity"]
                for lfa in range(num_lfas):
                    lambda_arn = path["LambdaFunctionAssociations"]["Items"][lfa]["LambdaFunctionARN"]
                    print(lambda_arn)
                    cursor.execute(f"INSERT OR IGNORE INTO lambdas (arn) VALUES ('{lambda_arn}')")
                    cursor.execute(f"SELECT rowid FROM lambdas WHERE arn = '{lambda_arn}'")
                    lambda_key = cursor.fetchone()[0]
                    print(lambda_key)
                    cursor.execute(f"INSERT INTO distros (id,lambda) VALUES ('{distro_id}',{lambda_key})")
                    cursor.execute(f"INSERT INTO paths (path, distro_id, lambda) VALUES ('{pattern}','{distro_id}',{lambda_key})")
                    num_lambdas += 1
    return num_lambdas
                


def main():
    # read in list of distro ids from csv file
    channel_list = pd.read_csv('batch1.csv')
    print(channel_list['cdn_dns'])
    # create sqlite db for results
    dbconn = sqlite3.connect('batch1.db')
    cursor = dbconn.cursor()
    # Check if on of the tables already exists so we don't overwrite them
    cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='distros'")
    distros_exists = False
    if cursor.fetchone()[0]==1:
        distros_exists = True

    if not distros_exists:
        cursor.execute("CREATE TABLE distros (id varchar(18) NOT NULL, lambda int)")
        cursor.execute("CREATE TABLE lambdas (arn varchar(255) NOT NULL UNIQUE)")
        cursor.execute("CREATE TABLE paths (path varchar(18) NOT NULL, distro_id VARCHAR(50) NOT NULL, lambda int)")

    # create cloudfront client
    client = boto3.client(
        "cloudfront",
        aws_access_key_id=AWS_CREDENTIALS["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=AWS_CREDENTIALS["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=AWS_CREDENTIALS.get("AWS_SESSION_TOKEN", None),
        region_name="us-east-1",
        config=Config(retries={"max_attempts": 10, "mode": "adaptive"}),
    )

    # foreach distro get the lambda function association it currently has
    channel_list['num_lambdas'] = channel_list.apply(lambda row : get_lambdas(client, row['cdn_dns'], cursor), axis=1)

    # spit out a table of all the LFAs and a count of channels using them
    # print(channel_list)

    cursor.execute("SELECT count(DISTINCT id) FROM distros")
    row_count = cursor.fetchone()[0]
    print(f"Number of Distros inserted: {row_count}")
    channel_list.to_csv('batch1_full.csv', sep='\t')
    cursor.execute("SELECT rowid, arn, (SELECT count(*) from distros WHERE lambda = lambdas.rowid GROUP BY lambda) num FROM lambdas ORDER BY arn desc")
    lambda_table = cursor.fetchall()
    print(*lambda_table, sep='\n')
    # Clean up
    dbconn.commit()
    dbconn.close()

if __name__ == "__main__":
    main()