# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
#1.	songplays - records in log data associated with song plays i.e. records with page NextSong
#•	songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL, 
    level VARCHAR NOT NULL, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INTEGER NOT NULL, 
    location VARCHAR, 
    user_agent VARCHAR);
""")
#2.	users - users in the app
#•	user_id, first_name, last_name, gender, level

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender VARCHAR, 
    level VARCHAR NOT NULL
    );
""")
#3.	songs - songs in music database
#•	song_id, title, artist_id, year, duration

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INTEGER, 
    duration FLOAT NOT NULL
    );
""")

#4.	artists - artists in music database
#•	artist_id, name, location, latitude, longitude

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR NOT NULL, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT
    );
""")
#5.	time - timestamps of records in songplays broken down INTEGERo specific units
#•	start_time, hour, day, week, month, year, weekday

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

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays 
(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users 
(user_id, first_name, last_name, gender, level)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (user_id)
                        DO UPDATE
                        SET level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs 
(song_id, title, artist_id, year, duration)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists 
(artist_id, name, location, latitude, longitude)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time 
(start_time, hour, day, week, month, year, weekday)
                        VALUES(%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING;
""")

# FIND SONGS
#Implement the song_select query in sql_queries.py to find the song ID and artist ID based on the title, artist name, and duration of a song.

song_select = ("""
    SELECT s.song_id, a.artist_id
    FROM songs as s
    JOIN artists as a ON s.artist_id = a.artist_id
    WHERE title = %s
    AND name = %s
    AND duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]




'''
    FOREIGN KEY (user_id)
    REFERENCES songplays (user_id)

    FOREIGN KEY (song_id)
    REFERENCES songplays (song_id)

    FOREIGN KEY (artist_id)
    REFERENCES songplays (artist_id)

    FOREIGN KEY (start_time)
    REFERENCES songplays (start_time)
'''