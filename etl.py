import logging
from config_loader import *
from sql_queries import copy_table_queries, insert_table_queries


logger = logging.getLogger(__name__)

db_config = DatabaseConfig.get_config('config/dwh.cfg')

def load_staging_tables(cur, conn):
    try:
        for query in copy_table_queries:
            logger.info(f"####### Loading {query=}".split("=")[0]+" #######")
            cur.execute(query)
            conn.commit()
    except Exception as err:
        logger.exception(err)
        raise(err)

def insert_tables(cur, conn):
    try:
        for query in insert_table_queries:
            logger.info(f"####### Inserting {query=}".split("=")[0]+" #######")
            cur.execute(query)
            conn.commit()
    except Exception as err:
        logger.exception(err)
        raise(err)