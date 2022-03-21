import pandas as pd
import boto3
import json
import configparser
import boto3
import time


def create_aws_resources(config):
    
    # Create the clients for EC2, AIM and Redshift using AWS_ACCESS_KEY and AWS_SECRET_KEY from dwh.cfg
    KEY = config['PWD']['KEY']
    SECRET = config['PWD']['SECRET']
     
    ec2 = boto3.resource('ec2', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET) 
    redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET) 

    return ec2, iam, redshift
    
    
def create_iam_role(iam, config):

    try:
         # Create the required IAM Role
        dwhRole = iam.create_role(Path='/',
                                    RoleName=config["CLUSTER_INFO"]["DWH_IAM_ROLE_NAME"],
                                    Description = "Allows Redshift clusters to call AWS services on your behalf.",
                                    AssumeRolePolicyDocument=json.dumps(
                                        {'Statement': [{'Action': 'sts:AssumeRole',
                                           'Effect': 'Allow',
                                           'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                         'Version': '2012-10-17'}))

         # Attach policy to required IAM Role
        iam.attach_role_policy(RoleName=config["CLUSTER_INFO"]["DWH_IAM_ROLE_NAME"],
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']

        roleArn = iam.get_role(RoleName=config["CLUSTER_INFO"]["DWH_IAM_ROLE_NAME"])['Role']['Arn']
        print('IAM Role:', roleArn)
    
    except Exception as e:
        print(e)
    
    return roleArn
   

def create_cluster(redshift, roleArn, config):

    # Creates Redshift cluster using parameters from dwh.cfg
    try:
        response = redshift.create_cluster(
        DBName=config['CLUSTER']['DWH_NAME'],
        ClusterIdentifier=config['CLUSTER_INFO']['DWH_CLUSTER_IDENTIFIER'],
        ClusterType=config['CLUSTER_INFO']['DWH_CLUSTER_TYPE'],
        NodeType=config['CLUSTER_INFO']['DWH_NODE_TYPE'],

        MasterUsername=config['CLUSTER']['DWH_USER'],
        MasterUserPassword=config['CLUSTER']['DWH_PASSWORD'],

        Port=int(config['CLUSTER']['DWH_PORT']),
        NumberOfNodes=int(config['CLUSTER_INFO']['DWH_NUM_NODES']),

        IamRoles=[roleArn],
    )
        print('Creating cluster')
        
    except Exception as e:
        print(e)
        

def open_tcp(cluster, ec2, config):
   
    # Open tcp connection to cluster
    try:
        vpc = ec2.Vpc(id=cluster['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config['CLUSTER']['DWH_PORT']),
            ToPort=int(config['CLUSTER']['DWH_PORT'])
        )
        
        print('TCP opened')
    except Exception as e:
        print(e)


def delete_iam(iam, config):
    
    try:
        # Delete IAM Role
        iam.detach_role_policy(RoleName=config.get("CLUSTER_INFO", "DWH_IAM_ROLE_NAME"), PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=config.get("CLUSTER_INFO", "DWH_IAM_ROLE_NAME"))
        print('Deleting Role')
    except Exception as e:
        print(e)
        
def delete_cluster(redshift, config):
    
    try:
        # Delete Redshift cluster
        redshift.delete_cluster(ClusterIdentifier=config['CLUSTER_INFO']['DWH_CLUSTER_IDENTIFIER'],  SkipFinalClusterSnapshot=True)
        print('Deleting Cluster')
    except Exception as e:
        print(e)
        
        
def main():                                                                                       
    
    # Read dwh.cfg file                                                                                   
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Create AWS Resources and Clients    
    ec2, iam, redshift = create_aws_resources(config)

    # Run requirement creations or delete roles and cluster if already exists
    try:   
        delete_cluster(redshift, config)
        delete_iam(iam, config)
        os._exit()
    except:
        roleArn = create_iam_role(iam,config)
        create_cluster(redshift, roleArn, config)

        # Loop until cluster status is available                                                                        
        t_interval = 60
        for t in range(int(600/t_interval)):
            cluster = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER_INFO', 'DWH_CLUSTER_IDENTIFIER'))['Clusters'][0]
            
            print('Cluster Status: ', cluster['ClusterStatus'])
            
            # Wait for cluster status
            if cluster['ClusterStatus'] == 'available':
                
                DWH_ENDPOINT = cluster['Endpoint']['Address']
                DWH_ROLE_ARN = cluster['IamRoles'][0]['IamRoleArn']
                print("dwf.cfg/CLUSTER/HOST: ", DWH_ENDPOINT)
                print("dwf.cfg/IAM_ROLE/ARN: ", DWH_ROLE_ARN)
                
                # Open TCP Connection to cluster
                open_tcp(cluster, ec2, config)
                break
            
            elif cluster['ClusterStatus'] == 'deleting':
                
                os._exit()
            
            else:
                time.sleep(t_interval)

if __name__ == "__main__":
    main()                                                                                       