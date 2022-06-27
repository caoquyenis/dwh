import configparser
import boto3

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

# Create clients for IAM, redshift
redshift = boto3.client('redshift',
                        region_name="us-east-1",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
#### CAREFUL!!
#-- Uncomment & run to delete the created resources
redshift.delete_cluster( ClusterIdentifier=config.get('DWH', 'DWH_CLUSTER_IDENTIFIER'),  SkipFinalClusterSnapshot=True)
#### CAREFUL!!