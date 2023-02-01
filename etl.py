import logging
from config_loader import *
from sql_queries import copy_table_queries, insert_table_queries


logger = logging.getLogger(__name__)

db_config = DatabaseConfig.get_config('config/dwh.cfg')

def load_staging_tables(cur, conn):
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