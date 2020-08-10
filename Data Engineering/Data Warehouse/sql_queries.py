import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS song"
artist_table_drop         = "DROP TABLE IF EXISTS artist"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" 
CREATE TABLE IF NOT EXISTS staging_events (
    event_id      INTEGER   IDENTITY(0,1) PRIMARY KEY,
    artist        VARCHAR,
    auth          VARCHAR,
    firstName     VARCHAR,
    gender        VARCHAR,
    itemInSession INTEGER,
    lastName      VARCHAR,
    length        DOUBLE PRECISION,
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR, 
    registration  BIGINT,
    sessionId     INTEGER,
    song          VARCHAR,
    status        INTEGER,
    ts            TIMESTAMP,
    userAgent     VARCHAR,
    userId        INTEGER
    );
""")


staging_songs_table_create = (""" 
CREATE TABLE IF NOT EXISTS staging_songs (
    songs_id INTEGER   IDENTITY(0,1) PRIMARY KEY,
    num_songs INTEGER NOT NULL,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR NOT NULL,
    song_id VARCHAR,
    title VARCHAR NOT NULL,
    duration FLOAT NOT NULL,
    year INTEGER
    );
""")

songplay_table_create = (""" 
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL, 
    level VARCHAR NOT NULL, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INTEGER NOT NULL, 
    location VARCHAR, 
    user_agent VARCHAR
    );
""")

user_table_create = (""" 
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender VARCHAR, 
    level VARCHAR NOT NULL
    );
""")

song_table_create = (""" 
CREATE TABLE IF NOT EXISTS song (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INTEGER, 
    duration FLOAT NOT NULL
    );
""")

artist_table_create = (""" 
CREATE TABLE IF NOT EXISTS artist (
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR NOT NULL, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT
    );
""")

time_table_create = (""" 
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL, 
    weekday INTEGER NOT NULL
    );
""")

# STAGING TABLES
"""
EXAMPLE
copy dwdate from 's3://awssampledbuswest2/ssbgz/dwdate' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';
"""
[LOG_DATA, LOG_JSONPATH, SONG_DATA] = config['S3'].values()

ARN = config["IAM_ROLE"]["ARN"]




staging_events_copy = (""" 
    COPY {}
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON {}
    region 'us-west-2';
""").format('staging_events', LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = (""" 
    COPY {}
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON 'auto'
    region 'us-west-2';
""").format('staging_songs', SONG_DATA, ARN)



# FINAL TABLES


songplay_table_insert = (""" 
INSERT INTO songplay ( 
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent)
    
SELECT  DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,
        se.userId          AS user_id, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.sessionId       AS session_id, 
        ss.artist_location AS location, 
        se.userAgent       AS user_agent
FROM staging_events se
JOIN staging_songs ss ON   (se.artist = ss.artist_name)
WHERE page = 'NextSong';
    
""")

user_table_insert = (""" 
INSERT INTO users (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level)

SELECT  userId    AS user_id, 
        firstName AS first_name, 
        lastName  AS last_name, 
        gender, 
        level
FROM staging_events
WHERE page = 'NextSong';
""")

song_table_insert = (""" 
INSERT INTO song (
    song_id, 
    title, 
    artist_id,
    year, 
    duration)
    
SELECT  song_id,
        title,
        artist_id,
        year,
        duration
FROM staging_songs;

    
""")

artist_table_insert = (""" 
INSERT INTO artist (
    artist_id,
    name, 
    location, 
    latitude, 
    longitude)

SELECT  artist_id,
        artist_name      AS name, 
        artist_location  AS location, 
        artist_latitude  AS latitude, 
        artist_longitude AS longitude
FROM staging_songs;
""")

time_table_insert = (""" 
INSERT INTO time (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday)

SELECT  ts                          AS start_time,
        EXTRACT(hr from ts)         AS hour,
        EXTRACT(d from ts)          AS day,
        EXTRACT(w from ts)          AS week,
        EXTRACT(mon from ts)        AS month,
        EXTRACT(yr from ts)         AS year, 
        EXTRACT(weekday from ts)    AS weekday 
FROM staging_events
WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
