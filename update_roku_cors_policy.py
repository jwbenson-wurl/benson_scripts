from operator import index
import pandas as pd
import boto3


class bcolors:
    OKGREEN  = '\033[92m'
    FAILRED  = '\033[91m'
    INFOPINK = '\033[35m'
    INFOCYAN = '\033[96m'
    ENDC     = '\033[0m'

# Initialize CloudFlare client
cf = boto3.client('cloudfront')

def RokuPolicySet(channel_config):

    if ("ResponseHeadersPolicyId" in channel_config["DistributionConfig"]["CacheBehaviors"]["Items"][2]):
        return True

    else:
        return False

# Import csv file to pandas dataframe
# NOTE: We're assuming we want to apply this configuration change
# to ALL of the channels in the CSV file

# TODO: Get the csv filename from argv[]

roku_dataframe = pd.read_csv('config_test.csv')
Roku_Allowed_Origins_PolicyID = "a59235a2-411a-4a4f-8ee0-290c28447d23"

# For channel_key in roku_dataframe
# Check if the Roku_Allowed_Origins policy is set
for index, channel in roku_dataframe.iterrows():

    channel_key = channel["channel_key"]
    distroID    = channel["distroID"]

    print("Channel: " + bcolors.INFOPINK + f"{channel_key}" + bcolors.ENDC)
    print("\tDistribution ID: " + bcolors.INFOCYAN + f"{distroID}" + bcolors.ENDC)

    try:
        channel_config = cf.get_distribution_config(Id=distroID)

    except cf.exceptions.NoSuchDistribution:

        print("\tChannel: " + bcolors.INFOPINK + f"{channel_key}" + 
            bcolors.ENDC + " with DistributionID " +
            bcolors.INFOCYAN + f"{distroID} " + 
            bcolors.FAILRED + "does not exist!" + bcolors.ENDC)
        roku_dataframe.loc[index, 'policySet'] = "FALSE"
        continue


    if (RokuPolicySet(channel_config)):

        rhpID = channel_config["DistributionConfig"]["CacheBehaviors"]["Items"][2]["ResponseHeadersPolicyId"]
        rhpConfig = cf.get_response_headers_policy_config(Id=rhpID)
        rhpName = rhpConfig["ResponseHeadersPolicyConfig"]["Name"]
        print("\tResponse Header Policy ID: " + bcolors.INFOCYAN + f"{rhpID}"
            + bcolors.ENDC)
        print("\tResponse Header Policy Name: " + bcolors.INFOCYAN + f"{rhpName}"
            + bcolors.ENDC)

        print(bcolors.OKGREEN + "\tCORS: TRUE" + bcolors.ENDC)
        roku_dataframe.loc[index, 'policySet'] = "TRUE"


    else:
        print(bcolors.FAILRED + "\tCORS: FALSE" + bcolors.ENDC)
        roku_dataframe.loc[index, 'policySet'] = "FALSE"

roku_dataframe.to_csv('config_test.csv',index=False)
