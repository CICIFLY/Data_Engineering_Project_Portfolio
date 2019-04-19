# DROP TABLES
# remember to write the whole sentence if exists
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"     # all use plural forms, it needs to be consistent with test.ipynb
user_table_drop = "DROP TABLE IF EXISTS users;"   # user is a reserved word
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"



# CREATE TABLES
# each table requires a primary key. when it does not have such a column, create a automatically increment one like (songplay_id # serial primary key and id serial primary key )

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (songplay_id serial primary key , start_time timestamp, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar ) """)

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (user_id int primary key, first_name varchar, last_name varchar, gender varchar, level varchar) """)

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (song_id varchar primary key, title varchar, artist_id varchar, year int, duration float) """)

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (artist_id varchar primary key, artist_name varchar, artist_location varchar, artist_lattitude float, artist_longitude float) """)

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (id serial primary key, start_time time without time zone NOT NULL, hour int NOT NULL, day int NOT NULL, week int NOT NULL, month int NOT NULL, year int NOT NULL, weekday int NOT NULL) """)



# INSERT RECORDS
# pay attention to three tables which requires to handle confliction because we set them as primary key (user_id,song_id,artist_id)

songplay_table_insert = (""" INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location , user_agent) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s) """)   
# songplay_id will increase automatically, it should not be treated as a separate column 

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING """)

artist_table_insert = (""" INSERT INTO artists (artist_id, artist_name, artist_location, artist_lattitude, artist_longitude)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING """)

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)""")


# FIND SONGS
# this is required in etl.ipynb 
# Implement the song_select query in sql_queries.py to find the song ID and artist ID based on the title, artist name, 
# and duration of a song.
#should be left join , some songs may not have artist in record
song_select = (""" SELECT s.song_id, a.artist_id
                   FROM songs s 
                   LEFT JOIN artists a     
                   ON s.artist_id = a.artist_id
                   WHERE title = (%s) AND artist_name = (%s) AND duration = (%s) """)    
# this where statement is based on what is required in etl.ipynb


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# reference:
# upsert example to handle conflict : http://www.postgresqltutorial.com/postgresql-upsert/