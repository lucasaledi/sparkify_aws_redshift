"""
THIS MODULE INCLUDES QUERIES USED IN CREATING/DROPING TABLES

Author: Lucas Aledi
Date: December 2022
"""
from helpers import *

# Gets Database configuration from dwh.cfg file
db_config = DatabaseConfig.get_config('config/dwh.cfg')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist              VARCHAR
    ,auth               VARCHAR(30)
    ,first_name         VARCHAR
    ,gender             VARCHAR(2)
    ,item_in_session    INT
    ,last_name          VARCHAR
    ,length             DECIMAL
    ,level              VARCHAR(15)
    ,location           VARCHAR
    ,method             VARCHAR(8)
    ,page               VARCHAR(20)
    ,registration       BIGINT
    ,session_id         INT
    ,song               VARCHAR
    ,status             INT
    ,ts                 TIMESTAMP
    ,user_agent         VARCHAR
    ,user_id            INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id           VARCHAR(30) NOT NULL
    ,artist_latitude    DECIMAL
    ,artist_longitude   DECIMAL
    ,artist_location    VARCHAR
    ,artist_name        VARCHAR
    ,num_songs          INT
    ,song_id            VARCHAR NOT NULL
    ,title              VARCHAR
    ,duration           DECIMAL
    ,year               INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id     INT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY
    ,start_time     TIMESTAMP
    ,user_id        INT
    ,level          VARCHAR(15)
    ,song_id        VARCHAR
    ,artist_id      VARCHAR(30)
    ,session_id     INT
    ,location       VARCHAR
    ,user_agent     VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id         INT NOT NULL SORTKEY
    ,first_name     VARCHAR 
    ,last_name      VARCHAR 
    ,gender         VARCHAR(2) 
    ,level          VARCHAR(15) 
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id         VARCHAR NOT NULL SORTKEY
    ,title          VARCHAR
    ,artist_id      VARCHAR(30)
    ,year           INT
    ,duration       DECIMAL
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id       VARCHAR(30) NOT NULL SORTKEY
    ,name           VARCHAR
    ,location       VARCHAR
    ,latitude       DECIMAL
    ,longitue       DECIMAL
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    time_id         INT IDENTITY(0,1)
    ,start_time     TIMESTAMP NOT NULL SORTKEY
    ,hour           INT NOT NULL
    ,day            INT NOT NULL
    ,week           INT NOT NULL
    ,month          INT NOT NULL
    ,year           INT NOT NULL
    ,weekday        INT NOT NULL
)
diststyle all;
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION{}
    FORMAT AS json {}
    TIMEFORMAT AS 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(db_config['s3_log_data']
            ,db_config['iam_role_arn']
            ,db_config['region']
            ,db_config['s3_log_metadata'])

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION {}
    FORMAT AS json 'auto ignorecase'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(db_config['s3_log_data']
            ,db_config['iam_role_arn']
            ,db_config['region'])


# ANALYTICS TABLES
## Note: IDENTITY columns (e.g., songplay_id) are automatically generated
songplay_table_insert = ("""
INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT 
        se.ts
        ,se.user_id
        ,se.level
        ,ss.song_id
        ,ss.artist_id
        ,se.session_id
        ,se.location
        ,se.user_agent
FROM    staging_events se
JOIN    staging_songs  ss
  ON    se.song = ss.title
 AND    se.artist = ss.artist_name
WHERE   se.page = 'NextSong'
 AND    se.user_id IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level)
SELECT DISTINCT
        user_id
        ,first_name
        ,last_name
        ,gender
        ,level
FROM    staging_eventes
WHERE   user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
SELECT DISTINCT
        song_id
        ,title
        ,artist_id
        ,year
        ,duration
FROM    staging_songs
WHERE   song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitue)
SELECT DISTINCT
        artist_id
        ,artist_name
        ,artist_location
        ,artist_latitude
        ,artist_longitude
FROM    staging_songs
WHERE   artist_id IS NOT NULL;
""")

## Note: IDENTITY columns (e.g., time_id) are automatically generated
time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT
        ts
        ,EXTRACT(HOUR FROM ts)
        ,EXTRACT(DAY FROM ts)
        ,EXTRACT(WEEK FROM ts)
        ,EXTRACT(MONTH FROM ts)
        ,EXTRACT(YEAR FROM ts)
        ,EXTRACT(DOW FROM ts)
FROM    staging_events;
""")


# QUERY LISTS
drop_table_queries = [staging_events_table_drop
                      ,staging_songs_table_drop
                      ,songplay_table_drop
                      ,user_table_drop
                      ,song_table_drop
                      ,artist_table_drop
                      ,time_table_drop]

create_table_queries = [staging_events_table_create
                        ,staging_songs_table_create
                        ,songplay_table_create
                        ,user_table_create
                        ,song_table_create
                        ,artist_table_create
                        ,time_table_create]

copy_table_queries = [staging_events_copy
                      ,staging_songs_copy]

insert_table_queries = [songplay_table_insert
                        ,user_table_insert
                        ,song_table_insert
                        ,artist_table_insert
                        ,time_table_insert]