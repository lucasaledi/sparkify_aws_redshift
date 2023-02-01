"""
THIS MODULE INCLUDES FUNCTIONS FOR DROPING AND CREATING STAGING TABLES

Author: Lucas Aledi
Date: December 2022
"""
from sql_queries import create_table_queries, drop_table_queries
import logging
from config_loader import *

# loads config
db_config = DatabaseConfig.get_config('config/dwh.cfg')

logger = logging.getLogger(__name__)

def drop_tables(cur, conn):
    """ Drops staging tables, if any
    
    Args:
    cur (obj):[object cursor created from the connection]
    conn (obj):[object connection created from psycopg2 to database]
    """
    try:
        logger.info("####### Droping Tables, if any. #######")
        i = 1
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
            logger.info(f"####### Droping {i} of {len(drop_table_queries)} #######")
            i += 1
    except Exception as err:
        logger.exception(err)
        raise(err)


def create_tables(cur, conn):
    """ Creates staging tables
    
    Args:
    cur (obj):[object cursor created from the connection]
    conn (obj):[object connection created from psycopg2 to database]
    """
    try:
        logger.info("####### Creating Tables #######")
        i = 1
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
            logger.info(f"####### Creating {i} of {len(create_table_queries)} #######")
            i += 1
    except Exception as err:
        logger.exception(err)
        raise(err)        