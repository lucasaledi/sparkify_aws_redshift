import logging
import json
import boto3
from botocore.exceptions import ClientError
from config_loader import *
from sql_queries import data_validation_queries

logger = logging.getLogger(__name__)

db_config = DatabaseConfig.get_config('config/dwh.cfg')

class IAM_role:
    def create_client(key,secret,region):
        """ Creates IAM client for User whose credentials are in the 
        config file
        
        Args:
        key (str):[string representing AWS access key ID]
        secret (str):[string representing AWS Secret Access key]
        region (str): [string representing region within AWS]
        
        Returns:
        iam (obj): [object representing IAM client]
        """
        try:
            logger.info("####### Accessing IAM User #######")
            iam = boto3.client('iam',aws_access_key_id=key,
                         aws_secret_access_key=secret,
                         region_name=region
                      )
            return iam
        except Exception as err:
            logger.exception(err)
            raise(err)
        
    def create_role(iam,iam_role_name):
        """ Using client created by create_client(), it creates 
        IAM role for User whose credentials are in the config file
        
        Args:
        iam (obj): [IAM client]
        iam_role_name (str): [string representing role name]
        """
        try:
            logger.info("####### Creating new IAM Role #######")
            logger.info(f"####### Name = {iam_role_name} #######")
            newRole = iam.create_role(
                Path = "/"
                ,RoleName=iam_role_name
                ,Description = "Allows access to Redshift"
                ,AssumeRolePolicyDocument = json.dumps(
                    {
                        'Statement' : [{
                            'Action' : 'sts:AssumeRole'
                            ,'Effect' : 'Allow'
                            ,'Principal' : {'Service' : 'redshift.amazonaws.com'}}],
                        'Version' : '2012-10-17'})
            )
        except iam.exceptions.EntityAlreadyExistsException as err:
            logger.info("####### NOTE: Role Already Exists!!! Continuing... #######")
            pass
        except Exception as err:
            logger.exception(err)
            raise(err)
    
    def attach_policy(iam,iam_role_name):
        """ Using client created by create_client(), it attaches a AdministratorAccess 
        policy to the IAM role created by create_role()
        
        Args:
        iam (obj): [IAM client]
        iam_role_name (str): [string representing role name]
        """
        try:
            logger.info("####### Attaching Policy to new IAM Role #######")
            iam.attach_role_policy(
                RoleName = iam_role_name
                ,PolicyArn = "arn:aws:iam::aws:policy/AdministratorAccess"
            )['ResponseMetadata']['HTTPStatusCode']
        except Exception as err:
            logger.exception(err)
            raise(err)
    
    def get_role_arn(iam,iam_role_name):
        """ Using client created by create_client(), it gets for the 
        role created by create_role() the respective ARN to be used
        when creating, e.g., a redshift cluster
        
        Args:
        iam (obj): [IAM client]
        iam_role_name (str): [string representing role name]
        
        Returns:
        roleARN (str): [string represent the role ARN]
        """
        try:
            logger.info("####### Fetching new IAM Role ARN #######")
            roleARN = iam.get_role(RoleName=iam_role_name)['Role']['Arn'] 
            return roleARN
        except Exception as err:
            logger.exception(err)
            raise(err)

class Redshift_cluster:    
    def create_client(key,secret,region):
        """ Creates Redshift client for User whose credentials are in the 
        config file
        
        Args:
        key (str):[string representing AWS access key ID]
        secret (str):[string representing AWS Secret Access key]
        region (str): [string representing region within AWS]
        
        Returns:
        redshift (obj): [object representing Redshift client]
        """
        try:
            logger.info("####### Creating Redshift Client #######")
            redshift = boto3.client('redshift',
                           region_name=region,
                           aws_access_key_id=key,
                           aws_secret_access_key=secret
                           )
            return redshift
        except Exception as err:
            logger.exception(err)
            raise(err)
    
    def create_cluster(redshift,db_config):
        """ Creates Redshift cluster for User whose credentials are in the 
        config file
        
        Args:
        redshift (obj):[Object representing Redshift Client]
        others (str): [strings given by dwh.cfg file]
        
        Returns:
        redshift (obj): [object representing Redshift client]
        """
        try:
            logger.info("####### Creating Redshift Cluster #######")
            response = redshift.create_cluster(
                # HW
                ClusterType = db_config['cluster_type']
                ,NodeType = db_config['node_type']
                ,NumberOfNodes = int(db_config['n_nodes'])
                # Identifiers & Credentials
                ,DBName = db_config['db_name']
                ,ClusterIdentifier = db_config['cluster_identifier']
                ,MasterUsername = db_config['db_user']
                ,MasterUserPassword = db_config['db_password']
                # Roles (for AdminAccess)
                ,IamRoles = [db_config['iam_role_arn']]
                # Make Cluster Public
                ,PubliclyAccessible = True ## added due to issues connecting to cluster
                # Associate Security Groups 
                ## added due to issues connecting to cluster
                ,VpcSecurityGroupIds = ['sg-065720dffeacbb1d2' # redshift_security_group
                                        ] 
            )
            return response
        except ClientError as err:
            logger.exception(err)
            pass
        except Exception as err:
            logger.exception(err)
            raise(err)

class Connect_cluster:
    def create_ec2_client(key,secret,region):
        """ Creates EC2 client for User whose credentials are in the 
        config file
        
        Args:
        key (str):[string representing AWS access key ID]
        secret (str):[string representing AWS Secret Access key]
        region (str): [string representing region within AWS]
        
        Returns:
        Eec2 (obj): [object representing IAM client]
        """
        try:
            logger.info("####### Creating EC2 Client #######")
            ec2 = boto3.resource('ec2',
                           region_name=region,
                           aws_access_key_id=key,
                           aws_secret_access_key=secret
                        )
            return ec2
        except Exception as err:
            logger.exception(err)
            raise(err)
    
    def attach_security_group(ec2,cluster_props,db_config):
        """ Attaches the security group to open an incoming TCP port to 
        access the cluster endpoint
    
        Args:
        ec2 (obj): [Boto3 ec2 object]
        cluster_props (dict): [Dictionary object of the defined properties]
        db_config (Configurations): [Class containing database configuration]
        """
        try:
            logger.info("####### Attaching Security Group. Opens an incoming TCP port to access the cluster endpoint. #######")
            vpc = ec2.Vpc(id=cluster_props['VpcId'])
            defaultSg = list(vpc.security_groups.all())[-1]
            #defaultSg = ec2.SecurityGroup(id='sg-00dfe631c59aaea17') # redshift_sg
            defaultSg.authorize_ingress(
                    #GroupName=defaultSg.group_name
                    GroupName='default'
                    ,CidrIp='0.0.0.0/0'
                    ,IpProtocol='TCP'
                    ,FromPort=int(db_config['db_port'])
                    ,ToPort=int(db_config['db_port'])
                    ,DryRun=True)
        except ClientError as err:
            if err.response['Error']['Code'] == 'InvalidPermission.Duplicate':
                logger.info("Policy already attached. Continuing...")
                pass
        except Exception as err:
            logger.exception(err)
            raise(err)

class Data_validation:
    def testing_queries(cur, conn):
        try:
            logger.info("####### Data Validation Test #######")
            for query in data_validation_queries:
                result = cur.execute(query)
                logger.info(f"####### {result} #######")
        except Exception as err:
            logger.exception(err)
            raise(err)