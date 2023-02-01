from sql_queries import create_table_queries, drop_table_queries
import logging
from config_loader import *

# Gets Database configuration from dwh.cfg file
db_config = DatabaseConfig.get_config('config/dwh.cfg')

logger = logging.getLogger(__name__)

def drop_tables(cur, conn):
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