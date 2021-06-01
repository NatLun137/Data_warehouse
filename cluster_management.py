import configparser
import boto3
import json
from threading import Timer
import logging

logger = logging.getLogger()
logging.basicConfig(format="[%(levelname)s] [%(asctime)s] %(message)s")
logger.setLevel(logging.INFO)


def connect_delayed():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    conn = "postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    logger.info(conn)


def delete_cluster():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=config.get('AWS','KEY'),
                       aws_secret_access_key=config.get('AWS','SECRET')
                       )
    redshift.delete_cluster(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"),  
                            SkipFinalClusterSnapshot=True)
    logger.info("Deteleting Redshift cluster.")
    
def create_cluster():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )

    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )

    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

    try:
        logger.info("1.1 Creating a new IAM Role.")
        iam.create_role(
            Path = '/',
            RoleName = DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift cluster to call AWS services on my behalf.",
            AssumeRolePolicyDocument = json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole', 'Effect': 'Allow', 
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )

    except Exception as e:
        print(e)

    try:
        logger.info("1.2 Attaching Policy.")
        iam.attach_role_policy(RoleName = DWH_IAM_ROLE_NAME,
                               PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print(e)

    logger.info("1.3 Get the IAM role ARN.")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    try:
        logger.info("Creating Redshift cluster.")
        redshift.create_cluster(        
            #DWH
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)


def main():
    
    create_cluster()
    logger.info("Connection to the cluster will be established in about 5 min.")
    Timer(300, connect_delayed).start()
    

if __name__ == "__main__":
    main()