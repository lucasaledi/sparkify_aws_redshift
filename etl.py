"""
THIS MODULE INCLUDES FUNCTIONS FOR THE ETL PROCESS

Author: Lucas Aledi
Date: December 2022
"""
import logging
from config_loader import *
from sql_queries import copy_table_queries, insert_table_queries

logger = logging.getLogger(__name__)

# loads config
db_config = DatabaseConfig.get_config('config/dwh.cfg')

def load_staging_tables(cur, conn):
    """ Copies data from source S3 bucket into staging tables
    
    Args:
    cur (obj):[object cursor created from the connection]
    conn (obj):[object connection created from psycopg2 to database]
    """
    try:
        i = 1
        for query in copy_table_queries:
            logger.info(f"####### Loading {i} of {len(copy_table_queries)}. This might take some time. #######")
            cur.execute(query)
            conn.commit()
            i += 1
    except Exception as err:
        logger.exception(err)
        raise(err)

def insert_tables(cur, conn):
    """ Insert data into tables to be used by Analytics Team
    
    Args:
    cur (obj):[object cursor created from the connection]
    conn (obj):[object connection created from psycopg2 to database]
    """    
    try:
        i = 1
        for query in insert_table_queries:
            logger.info(f"####### Inserting {i} of {len(insert_table_queries)} #######")
            cur.execute(query)
            conn.commit()
            i += 1
    except Exception as err:
        logger.exception(err)
        raise(err)