import configparser
import boto3


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# CREATE SCHEMAS
fact_schema = ("CREATE SCHEMA IF NOT EXISTS fact_tables")
dimension_schema = ("CREATE SCHEMA IF NOT EXISTS dimension_tables")
staging_schema = ("CREATE SCHEMA IF NOT EXISTS staging_tables")

# DROP SCHEMAS
fact_schema_drop= ("DROP SCHEMA IF EXISTS fact_tables CASCADE")
dimension_schema_drop= ("DROP SCHEMA IF EXISTS dimension_tables CASCADE")
staging_schema_drop= ("DROP SCHEMA IF EXISTS staging_tables CASCADE")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_tables.staging_events_table (
    "artist" text,
    "auth" text,
    "firstName" text,
    "gender" text,
    "itemInSession" int,
    "lastName" text,
    "length" numeric,
    "level" text,
    "location" text,
    "method" text,
    "page" text,
    "registration" numeric,
    "sessionId" int,
    "song" text,
    "status" int,
    "ts" bigint,
    "userAgent"text,
    "userId" text,
    "start_time" bigint,
    "hour" int,
    "day" int,
    "week" int,
    "month" int,
    "year" int,
    "weekday" int)
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_tables.staging_songs_table (
    "artist_id" text,
    "artist_latitude" numeric,
    "artist_location" text,
    "artist_longitude" numeric,
    "artist_name" text,
    "duration" numeric,
    "num_songs" numeric,
    "song_id" text,
    "title" text,
    "year" numeric)
""")

songplay_table_create = ("""
    CREATE TABLE fact_tables.songplays (
    songplay_id bigint IDENTITY(0,1) NOT NULL PRIMARY KEY,
    start_time bigint NOT NULL sortkey,
    user_id text distkey,
    level text,
    song_id text,
    artist_id text,
    session_id int,
    location text,
    user_agent text)
""")

user_table_create = ("""
    CREATE TABLE dimension_tables.users (
    user_id text NOT NULL PRIMARY KEY sortkey,
    first_name text,
    last_name text,
    gender text,
    level text)
""")

song_table_create = ("""
    CREATE TABLE dimension_tables.songs (
    song_id text NOT NULL PRIMARY KEY sortkey,
    title text,
    artist_id text,
    year int,
    duration numeric)
""")

artist_table_create = ("""
    CREATE TABLE dimension_tables.artists (
    artist_id text NOT NULL PRIMARY KEY sortkey,
    name text,
    location text,
    latitude numeric,
    longitude numeric)
""")

time_table_create = ("""
    CREATE TABLE dimension_tables.time (
    start_time bigint NOT NULL PRIMARY KEY sortkey,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int)
""")

iam = boto3.client('iam',aws_access_key_id=config.get('AWS','KEY'),
                   aws_secret_access_key=config.get('AWS','SECRET'),
                   region_name='us-west-2')
roleArn = iam.get_role(RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"))['Role']['Arn']

# STAGING TABLES

staging_songs_copy = ("""
    copy staging_tables.staging_songs_table from {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto' ACCEPTINVCHARS;
""").format(config.get('S3', 'SONG_DATA'), roleArn)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO fact_tables.songplays (start_time, 
        user_id,level,song_id,artist_id,session_id,location,user_agent)
    (SELECT e.start_time, e.userId as user_id, e.level, s.song_id, 
          s.artist_id, e.sessionId as session_id, e.location, e.userAgent as user_agent
    FROM staging_tables.staging_events_table e
    JOIN staging_tables.staging_songs_table s
    ON (e.song = s.title)
    WHERE s.song_id IS NOT NULL)
""")

user_table_insert = ("""
    INSERT INTO dimension_tables.users
    (SELECT DISTINCT e.userId as user_id, e.firstName as first_name,
        e.lastName as last_name, e.gender, e.level
    FROM staging_tables.staging_events_table e
    WHERE e.userId IS NOT NULL)
""")

song_table_insert = ("""
    INSERT INTO dimension_tables.songs
    (SELECT DISTINCT s.song_id, s.title, s.artist_id, s.year, s.duration
    FROM staging_tables.staging_songs_table s
    WHERE s.song_id IS NOT NULL)
""")

artist_table_insert = ("""
    INSERT INTO dimension_tables.artists
    (SELECT DISTINCT s.artist_id, s.artist_name as name, s.artist_location as location,
        s.artist_latitude as lattitude, s.artist_longitude as longitude
    FROM staging_tables.staging_songs_table s
    WHERE s.artist_id IS NOT NULL)
""")

time_table_insert = ("""
    INSERT INTO dimension_tables.time
    (SELECT DISTINCT e.start_time, e.hour, e.day, e.week, e.month, e.year, e.weekday
    FROM staging_events_table e
    WHERE e.start_time IS NOT NULL)
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_schemas_queries = [fact_schema_drop,dimension_schema_drop,staging_schema_drop]
create_schemas_queries = [fact_schema,dimension_schema,staging_schema]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]