import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN" )

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= """
CREATE TABLE IF NOT EXISTS staging_events(
    artist          text,
    auth            text,
    firstName       text,
    gender          text,
    iteminSession   int,
    lastName        text,
    length          numeric,
    level           text,
    location        text,
    method          text,
    page            text,
    registration    numeric,
    sessionId       int,
    song            text,
    status          int,
    ts              timestamp,
    userAgent       text,
    userId          int
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id             text     primary key,
    num_songs           int,
    artist_id           text,
    artist_latitude     numeric,
    artist longitude    numeric,
    artist location     text,
    artist name         text,
    title               text,
    duration            numeric,
    year                int
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id     varchar     primary key     identity(0,1),
    start_time      bigint      not null        sortkey         distkey,
    user_id         text        not null,
    level           text,
    song_id         text        not null,
    artist_id       text        not null,
    session_id      int,
    location        text,
    user_agent      text
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users(
    user_id     int     not null primary key,
    first_name  text,
    last_name   text,
    gender      text,
    level       text
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs(
    song_id     text    not null    primary key,
    title       text    not null,
    artist_id   text    not null,
    year        int,
    duration    int
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists(
    artist_id   text    not null    primary key,
    name        text,
    location    text,
    latitude    numeric,
    longitude   numeric
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time(
    start_time  timestamp   not null distkey sortkey primary key,
    hour        int         not null,
    day         int         not null,
    week        int         not null,
    month       int         not null,
    year        int         not null,
    weekday     int         not null
)
"""

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
    credentials 'aws_iam_role={}'
    gzip region 'us-west-2'
    format as JSON {}
    timeformat as 'epochmillisecs';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {} 
    credentials 'aws_iam_role={}'
    gzip region 'us-west-2'
    format as JSON 'auto';
""").format(SONG_DATA, IAM_ROLE)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + ((e.ts / 1000) * INTERVAL '1 second') AS start_time,
        e.userId AS user_id,
        e.level AS level,
        s.song_id AS song_id,
        s.artist_id AS artist_id,
        e.sessionId AS session_id,
        e.location AS location,
        e.userAgent AS user_agent
    FROM staging_events AS e
    JOIN staging_songs AS s
    ON e.artist = s.artist_name AND e.song = s.title AND e.page = 'NextSong' AND e.length = s.duration;
""")

user_table_insert = ("""
INSERT INTO users (
    user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender AS gender,
        level AS level
    FROM staging_events
    WHERE user_id IS NOT NULL AND page = 'NextSong
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) AS song_id,
        title AS title,
        artist_id AS artist_id,
        year AS year,
        duration AS duration
    FROM staging_songs
    WHERE song_id IS NOT NULL 
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time 
    WITH cte AS (
        SELECT TIMESTAMP 'epoch' + ((ts/1000) * INTERVAL '1 second') AS ts 
        FROM staging_events)
    SELECT DISTINCT(ts) AS start_time,
        EXTRACT(hour from ts),
        EXTRACT(day from ts),
        EXTRACT(week from ts),
        EXTRACT(month from ts),
        EXTRACT(year from ts),
        EXTRACT(weekday from ts)
    FROM cte
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
