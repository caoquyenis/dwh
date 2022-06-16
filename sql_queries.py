import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplays_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id bigint PRIMARY KEY NOT NULL,
        artist VARCHAR NOT NULL,
        auth VARCHAR NOT NULL,
        firstName VARCHAR NOT NULL,
        gender VARCHAR NOT NULL,
        itemInSession INT NOT NULL,
        lastName VARCHAR NOT NULL,
        length FLOAT,
        level VARCHAR NOT NULL,
        location VARCHAR NOT NULL,
        method VARCHAR NOT NULL,
        page VARCHAR NOT NULL,
        registration FLOAT NOT NULL,
        sessionId INT NOT NULL,
        song VARCHAR NOT NULL,
        status INT NOT NULL,
        ts BIGINT NOT NULL,
        userAgent VARCHAR NOT NULL,
        userId INT NOT NULL
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs bigint PRIMARY KEY NOT NULL,
        artist_id VARCHAR NOT NULL,
        artist_latitude FLOAT,
        artist_logitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR NOT NULL,
        title VARCHAR,
        duration FLOAT NOT NULL,
        year INT NOT NULL
    )
""")
# Fact table creating
songplays_table_create = ("""
    CREATE TABLE IF NOT EXISTS songsplays (
        songsplay_id        int IDENTITY(0,1) PRIMARY KEY NOT NULL,
        start_time          BIGINT NOT NULL,
        user_id             INT NOT NULL,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR,
        session_id          VARCHAR,
        location            VARCHAR,
        user_agent          VARCHAR
    )
""")
# Dimension tables creating
users_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY NOT NULL,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    )
""")

songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY NOT NULL,
        title TEXT NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT,
        duration FLOAT NOT NULL
    )
""")

artists_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY NOT NULL,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time          BIGINT PRIMARY KEY NOT NULL,
        hour                INTEGER,
        day                 INTEGER,
        week                INTEGER,
        month               INTEGER,
        year                INTEGER,
        weekday             INTEGER
    )
""")

# STAGING TABLES
# Copy log data json file on S3: s3://udacity-dend/log_data
staging_events_copy = ("""
    COPY staging_events FROM 's3://udacity-dend/log_data'
    CREDENTIALS 'aws_iam_role={}'
    gzip delimiter ';' compupdate off region 'us-west-2';
""").format('arn:aws:iam::501200385798:role/dwhRole')

staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={}'
    gzip delimiter ';' compupdate off region 'us-west-2';
""").format('arn:aws:iam::501200385798:role/dwhRole')

# FINAL TABLES

songplays_table_insert = ("""
    INSERT INTO songsplays (
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

users_table_insert = ("""
    INSERT INTO users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    ) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
""")

songs_table_insert = ("""
    INSERT INTO songs (
        song_id, 
        title,
        artist_id, 
        year, 
        duration
    ) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING
""")

artists_table_insert = ("""
    INSERT INTO artists (
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    ) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplays_table_create, users_table_create, songs_table_create, artists_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplays_table_insert, users_table_insert,
                        songs_table_insert, artists_table_insert, time_table_insert]
# Table test

redshift_select = ("""
SELECT *
FROM dev
""")