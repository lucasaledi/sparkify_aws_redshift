from dataclasses import dataclass
import configparser

class DatabaseConfig:
    """
    Dataclass used for storing information and returning it for 
    the main cycle of the ETL process under sparkify_dwh_etl_pipeline.py
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
        iam_role_name: str
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
            key=config.get("AWS", "KEY")
            ,secret=config.get("AWS", "SECRET")
            ,region=config.get("AWS", "REGION")
            ,iam_role_arn=config.get("IAM_ROLE","ARN") # S3 readOnly IAM role
            ,cluster_type=config.get("CLUSTER", "DB_CLUSTER_TYPE")
            ,n_nodes=config.get("CLUSTER", "DB_NUM_NODES")
            ,node_type=config.get("CLUSTER", "DB_NODE_TYPE")
            ,iam_role_name=config.get("CLUSTER", "DB_IAM_ROLE_NAME")
            ,cluster_identifier=config.get("CLUSTER", "DB_CLUSTER_IDENTIFIER")
            ,db_name=config.get("CLUSTER", "DB_NAME")
            ,db_user=config.get("CLUSTER", "DB_USER")
            ,db_password=config.get("CLUSTER", "DB_PASSWORD")
            ,db_port=config.get("CLUSTER", "DB_PORT")
            ,db_host=config.get("CLUSTER","HOST")
            ,db_iam_role_arn=config.get("CLUSTER","DB_IAM_ROLE_ARN") # Redshift Cluster
            ,s3_log_data=config.get("S3","LOG_DATA")
            ,s3_log_metadata=config.get("S3","LOG_JSONPATH")
            ,s3_song_data=config.get("S3","SONG_DATA")
        ).__dict__
        if None in db_config.values() or "" in db_config.values():
            return -1
        return db_config