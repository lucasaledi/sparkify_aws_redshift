"""
THIS MODULE INCLUDES CODE TO LOAD REDSHIFT CLUSTER CONFIGURATION

Author: Lucas Aledi
Date: December 2022
"""
from dataclasses import dataclass
import configparser

class DatabaseConfig:
    """
    Dataclass used for storing information and returning it for 
    the main cycle of the ETL process under sparkify_redshift_db.py,
    aws_functions.py, create_tables.py and etl.py
    """
    @dataclass
    class Configurations:
        """ 
        Dataclass used for storing info from dwh.cfg
        """
        key: str
        secret: str
        region: str
        iam_role_arn: str
        cluster_type: str
        n_nodes: str
        node_type: str
        db_iam_role_name: str
        cluster_identifier: str
        db_name: str
        db_user: str
        db_password: str
        db_port: str
        db_host: str
        db_iam_role_arn: str
        s3_log_data: str
        s3_log_metadata: str
        s3_song_data: str
    
    def get_config(path_to_file):
        """ Storing dwh.cfg information into Configurations dataclass
        Args:
        path_to_file (str):[string representing path to config file]

        Returns dict with key value pairs
        """
        config = configparser.ConfigParser()
        config.read(path_to_file)
        db_config = DatabaseConfig.Configurations(
            key=config.get("AWS", "key")
            ,secret=config.get("AWS", "secret")
            ,region=config.get("AWS", "region")
            ,iam_role_arn=config.get("IAM_ROLE","arn") # IAM role
            ,cluster_type=config.get("CLUSTER", "db_cluster_type")
            ,n_nodes=config.get("CLUSTER", "db_num_nodes")
            ,node_type=config.get("CLUSTER", "db_node_type")
            ,db_iam_role_name=config.get("CLUSTER", "db_iam_role_name")
            ,cluster_identifier=config.get("CLUSTER", "db_cluster_identifier")
            ,db_name=config.get("CLUSTER", "db_name")
            ,db_user=config.get("CLUSTER", "db_user")
            ,db_password=config.get("CLUSTER", "db_password")
            ,db_port=config.get("CLUSTER", "db_port")
            ,db_host=config.get("CLUSTER","host")
            ,db_iam_role_arn=config.get("CLUSTER","db_iam_role_arn") # Redshift Cluster
            ,s3_log_data=config.get("S3","log_data")
            ,s3_log_metadata=config.get("S3","log_jsonpath")
            ,s3_song_data=config.get("S3","song_data")
        ).__dict__
        return db_config