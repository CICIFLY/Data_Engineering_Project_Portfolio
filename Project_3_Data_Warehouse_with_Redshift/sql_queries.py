import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



# DROP TABLES
# do not need quote characters for staging tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"



# CREATE TABLES
# every table should have a primary key 
# I deleted all the 'if not exists' in table creating statement 

staging_events_table_create= ("""CREATE TABLE staging_events (
artist_name varchar, 
auth varchar,
user_first_name varchar, 
gender varchar, 
itemInSession int, 
user_last_name varchar, 
length numeric, 
level varchar, 
user_location varchar, 
method varchar, 
page varchar, 
registration bigint, 
sessionId int, 
song_title varchar, 
status int, 
ts bigint, 
userAgent varchar, 
user_id int)""");
# changed ts to start_time to be consistent



staging_songs_table_create = ("""CREATE TABLE staging_songs(
num_songs int, 
artist_id varchar, 
artist_latitude numeric, 
artist_longitude numeric, 
artist_location varchar, 
artist_name varchar, 
song_id varchar, 
song_title varchar, 
duration numeric, 
year int)
""");



songplay_table_create = ("""CREATE TABLE songplays ( 
songplay_id INT IDENTITY(0,1) PRIMARY KEY ,  
start_time TIMESTAMP NOT NULL REFERENCES times(start_time), 
user_id INT NOT NULL REFERENCES users(user_id), 
level VARCHAR, 
song_id VARCHAR NOT NULL REFERENCES songs(song_id), 
artist_id VARCHAR NOT NULL REFERENCES artists(artist_id), 
sessionId INT ,
user_location VARCHAR, 
userAgent VARCHAR
)
""");

# four references not null removed 


user_table_create = ("""CREATE TABLE users  (
user_id INT PRIMARY KEY, 
user_first_name VARCHAR, 
user_last_name VARCHAR, 
gender VARCHAR,  
level VARCHAR
)
""");



song_table_create = ("""CREATE TABLE songs (
song_id VARCHAR PRIMARY KEY, 
song_title VARCHAR,
artist_id VARCHAR NOT NULL, 
year INTEGER, 
duration FLOAT
)
""");



artist_table_create = ("""CREATE TABLE artists (
artist_id VARCHAR PRIMARY KEY, 
artist_name VARCHAR, 
artist_location VARCHAR, 
artist_latitude FLOAT, 
artist_longitude FLOAT
)
""");



time_table_create = ("""CREATE TABLE times (
start_time TIMESTAMP PRIMARY KEY, 
hour INTEGER , 
day INTEGER,
week INTEGER, 
month VARCHAR, 
year INTEGER, 
weekday VARCHAR
)
""");
# changed INT to INTEGER

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')

# To load the sample data, you must provide authentication for your cluster to access Amazon S3 on your behalf. 
# You can provide either role-based authentication or key-based authentication. In this case it is role-based authentication

# setting COMPUPDATE, STATUPDATE to speed up COPY
staging_events_copy = (""" copy staging_events from {}
                           iam_role {}
                           region 'us-west-2' 
                           COMPUPDATE OFF STATUPDATE OFF
                           JSON {} timeformat 'epochmillisecs'; """).format(LOG_DATA, ARN, LOG_JSONPATH )
# completed: 8056 rows for events



staging_songs_copy = (""" copy staging_songs from {}
                         iam_role {}
                         region 'us-west-2' 
                         COMPUPDATE OFF STATUPDATE OFF
                         JSON 'auto' """).format(SONG_DATA,ARN)
# completed : 14896 rows for songs

 
    
# FINAL TABLES
# make sure when selecting columns, the orders of the columns should be the same as previous order

# remember songplay_id is auto incremented so do not need to add here
songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, sessionId, user_location, userAgent) 
SELECT DISTINCT 
                TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time,
                e.user_id, 
                e.level, 
                s.song_id, 
                s.artist_id, 
                e.sessionId, 
                e.user_location, 
                e.userAgent
FROM staging_events e
JOIN staging_songs s
ON e.song_title = s.song_title
WHERE e.page = 'NextSong' AND e.user_id NOT IN (SELECT DISTINCT sp.user_id FROM songplays sp WHERE sp.user_id =  e.user_id AND sp.start_time = e.ts AND sp.sessionId = e.sessionId)
""")
# here pay attention to the alias. Do not make two s . The program will be confused


user_table_insert = (""" INSERT INTO users (user_id, user_first_name, user_last_name, gender, level) 
SELECT DISTINCT 
               user_id, 
               user_first_name, 
               user_last_name, 
               gender, 
               level
FROM staging_events 
WHERE page = 'NextSong' AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")



# do not need WHERE paging = 'NextSong' coz it has nothing to do with staging_events table
song_table_insert = (""" INSERT INTO songs (song_id, song_title, artist_id, year, duration) 
SELECT DISTINCT 
               song_id, 
               song_title, 
               artist_id, 
               year, 
               duration
FROM staging_songs
WHERE song_id NOT IN ( SELECT DISTINCT song_id FROM songs)
""")



artist_table_insert = (""" INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude) 
SELECT DISTINCT                                                                          
               artist_id, 
               artist_name, 
               artist_location, 
               artist_latitude, 
               artist_longitude
FROM staging_songs
WHERE artist_id NOT IN (SELECT DISTINCT artist_id  FROM artists)
""")



# do not need WHERE paging = 'NextSong' coz it has nothing to do with staging_events table

# in order to use extarct(), we have to have ts as varchar 

time_table_insert = ("""
INSERT INTO times \
(start_time, hour, day, week, month, year, weekday) \
SELECT start_time, \
    EXTRACT(hr from start_time), \
    EXTRACT(d from start_time), \
    EXTRACT(w from start_time), \
    EXTRACT(mon from start_time), \
    EXTRACT(yr from start_time), \
    EXTRACT(weekday from start_time) \
FROM (SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time \
      FROM staging_events s)""")



# QUERY LISTS
# When creating tables, we should create dimension tables first, then fact table
create_table_queries = [staging_events_table_create, staging_songs_table_create,user_table_create, song_table_create, artist_table_create,time_table_create,songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]