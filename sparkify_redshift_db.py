import sys
import psycopg2
from configparser import ConfigParser
import logging
import create_tables
from etl import *
from helpers import *

# Logger for debugging
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

# ETL workflow 
def main():
    try:
        # Gets Database configuration from dwh.cfg file
        db_config = DatabaseConfig.get_config('config/dwh.cfg')

        # Creates S3 ReadOnly IAM role
        logger.info("======= Creating IAM Role =======")
        iam = IAM_role.create_client(db_config['key'],db_config['secret'],db_config['region'])        
        IAM_role.create_role(iam,db_config['db_iam_role_name'])
        IAM_role.attach_policy(iam,db_config['db_iam_role_name'])
        role_arn = IAM_role.get_role_arn(iam,db_config['db_iam_role_name'])
        # Inserts S3 IAM Role ARN into db_config
        db_config['iam_role_arn'] = role_arn
        # Set variable for edits to the config file
        edit = ConfigParser()
        edit.read('config/dwh.cfg')
        edit_iamRole = edit['IAM_ROLE']
        edit_iamRole['arn'] = role_arn
        with open('config/dwh.cfg','w') as f:
            edit.write(f)
            f.close()
        # Gets Database configuration from dwh.cfg file WITH UPDATE
        db_config = DatabaseConfig.get_config('config/dwh.cfg')

        logger.info(f"======= iam_role_arn : {db_config['iam_role_arn']} =======")

        # Creates Redshift Cluster
        logger.info("======= Creating Redshift Cluster =======")
        redshift = Redshift_cluster.create_client(db_config['key'],db_config['secret'],db_config['region'])
        response = Redshift_cluster.create_cluster(redshift,db_config)
        # Tests cluster creation
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            cluster_props = redshift.describe_clusters(
                    ClusterIdentifier=db_config['cluster_identifier']
                    )['Clusters'][0]
            cluster_status = cluster_props['ClusterStatus'].lower()
            # Tests cluster availability
            while cluster_status != 'available':
                logger.info(f'Cluster status is {cluster_status}. This might take a moment.')
                cluster_props = redshift.describe_clusters(
                    ClusterIdentifier=db_config['cluster_identifier']
                    )['Clusters'][0]
                cluster_status = cluster_props['ClusterStatus'].lower()
                sleep(30)
            # Once cluster is available...
            cluster_props = redshift.describe_clusters(
                    ClusterIdentifier=db_config['cluster_identifier']
                    )['Clusters'][0]
            cluster_status = cluster_props['ClusterStatus'].lower()
            logger.info(f'Cluster is now live. Status {cluster_status}')
            # Gets end point to be used for connection. Update db_config
            end_point = cluster_props['Endpoint']['Address']
            db_config['db_host'] = end_point
            # Gets Redshift Cluster IAM Role ARN. Update db_config
            redshift_IamRoleArn = cluster_props['IamRoles'][0]['IamRoleArn']
            db_config['db_iam_role_arn'] = redshift_IamRoleArn
            logger.info(f"======= db_iam_role_arn : {cluster_props} =======")
        else:
            logger.info(f"Something went wrong while creating Redshift Cluster.\n \
                        Code returned: {response['ResponseMetadata']['HTTPStatusCode']}")

        # CONNECTING TO DATABASE
        # Creates EC2 client
        ec2 = Connect_cluster.create_ec2_client(db_config['key']
                                                ,db_config['secret']
                                                ,db_config['region'])
        # Open TCP port
        Connect_cluster.attach_security_group(ec2,cluster_props,db_config)
        
        logger.info(f"======= Connecting to Database {db_config['db_name']} =======")
        conn_string = "host={} dbname={} user={} password={} port={}".format(
            db_config['db_host']
            ,db_config['db_name']
            ,db_config['db_user']
            ,db_config['db_password']
            ,db_config['db_port']
            )
        logger.info(f"======= {conn_string} =======")
        
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        logger.info("======= Creating Staging & Analytics tables =======")
        create_tables.drop_tables(cur, conn)
        create_tables.create_tables(cur, conn)

        logger.info("======= Loading Staging Tables =======")
        logger.info(db_config['iam_role_arn'])
        load_staging_tables(cur, conn)

        logger.info("======= Inserting data from Staging tables to Analytics =======")
        insert_tables(cur, conn)

    except Exception as err:
        logger.info("======= Exception name:" + err.__class__.__name__ + " =======")
        logger.exception(err)
        sys.exit(1)
    
    finally:
        cur.close()
        logger.info(f"======= Closed Cursor =======")
        conn.close()
        logger.info(f"======= Closed connection with Database {db_config['db_name']} =======")
        redshift.delete_cluster(ClusterIdentifier=db_config['cluster_identifier']
                                ,SkipFinalClusterSnapshot=True)
        logger.info(f"======= Deleted Cluster  =======")
        iam.detach_role_policy(RoleName=db_config['db_iam_role_name'], PolicyArn='arn:aws:iam::aws:policy/AdministratorAccess')
        logger.info(f"======= Detached Role Policy  =======")
        iam.delete_role(RoleName=db_config['db_iam_role_name'])
        logger.info(f"======= Deleted Role  =======")
        logger.info(f"======= All done  =======")

if __name__ == '__main__':
    main()