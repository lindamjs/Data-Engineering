# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
(songplay_id SERIAL PRIMARY KEY, start_tm timestamp NOT NULL, user_id varchar NOT NULL, level varchar, 
song_id varchar, artist_id varchar, session_id int, location text,
user_agent varchar)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id varchar PRIMARY KEY, 
first_name varchar, last_name varchar, gender char, level varchar)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY, 
song_name varchar, artist_id varchar, year int, duration numeric)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY, 
artist_name varchar, location varchar, lattitude varchar, longitude varchar)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(time time PRIMARY KEY, hour int, 
day int, week int, month int, year int, weekday int)""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(start_tm, user_id, 
level, song_id, artist_id, session_id, location, user_agent)           
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(user_id) DO UPDATE SET level = excluded.level;""")

song_table_insert = ("""INSERT INTO songs(song_id, song_name, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(song_id) DO NOTHING;""")

artist_table_insert = ("""INSERT INTO artists(artist_id, artist_name, location, lattitude, longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(artist_id) DO NOTHING;""")

time_table_insert = ("""INSERT INTO time(time, hour, day, week, month, year, weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(time) DO NOTHING;""")

# FIND SONGS

song_select = ("""SELECT s.song_id, a.artist_id FROM songs s, artists a 
WHERE s.song_name=%s and a.artist_name=%s and s.duration=%s""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]