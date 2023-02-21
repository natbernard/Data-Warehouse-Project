import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF TABLE EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF TABLE EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF TABLE EXISTS songplays;"
user_table_drop = "DROP TABLE IF TABLE EXISTS users;"
song_table_drop = "DROP TABLE IF TABLE EXISTS songs;"
artist_table_drop = "DROP TABLE IF TABLE EXISTS artists;"
time_table_drop = "DROP TABLE IF TABLE EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist text
    auth text
    firstName text
    gender text
    iteminSession int
    lastName text
    length numeric
    level text
    location text
    method text
    page text
    registration numeric
    sessionId int
    song text
    status int
    ts timestamp
    userAgent text
    userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs int
    artist_id text
    artist_latitude numeric
    artist longitude numeric
    artist location text
    artist name text
    song_id text
    title text
    duration numeric
    year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    songplay_id varchar
    start_time bigint
    user_id text
    level text
    song_id text
    artist_id text
    session_id int
    location text
    user_agent text
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id int
    first_name text
    last_name text
    gender text
    level text
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id text
    title text
    artist_id text
    year int
    duration int
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    artist_id text
    name text
    location text
    latitude numeric
    longitude numeric
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    start_time timestamp
    hour int
    day int
    week int
    month int
    year int
    weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
copy staging_songs from {} 
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    songplay_id
    start_time
    user_id
    level
    song_id
    artist_id
    session_id
    location
    user_agent
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (
    user_id
    first_name
    last_name
    gender
    level
) VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id
    title
    artist_id
    year
    duration
) VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id
    name
    location
    latitude
    longitude
) VALUES (%s, %s, %s, %s, %s)
""")

time_table_insert = ("""
INSERT INTO time (
    start_time
    hour
    day
    week
    mont
    year
    weekday
) VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
