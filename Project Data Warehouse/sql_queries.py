import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
        
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                    artist TEXT,
                                    auth TEXT,
                                    firstName TEXT,
                                    gender TEXT,
                                    itemInSession INTEGER,
                                    lastName TEXT,
                                    length FLOAT4,
                                    level TEXT,
                                    location TEXT,
                                    method TEXT,
                                    page TEXT,
                                    registration NUMERIC,
                                    sessionid INTEGER,
                                    song TEXT,
                                    status INTEGER,
                                    ts BIGINT,
                                    userAgent TEXT,
                                    userId INTEGER
                            )
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs INTEGER, 
                                    artist_id TEXT, 
                                    artist_latitude NUMERIC,  
                                    artist_longitude NUMERIC,
                                    artist_location TEXT, 
                                    artist_name TEXT, 
                                    song_id TEXT, 
                                    title TEXT, 
                                    duration FLOAT4, 
                                    year INTEGER)
                            """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table (
                                    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
                                    start_time TIMESTAMP SORTKEY,
                                    user_id TEXT DISTKEY,
                                    level TEXT,
                                    song_id TEXT,
                                    artist_id TEXT,
                                    session_id INTEGER,
                                    location TEXT,
                                    user_agent TEXT) diststyle key
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table (
                                    user_id TEXT PRIMARY KEY SORTKEY,
                                    first_name TEXT,
                                    last_name TEXT,
                                    gender TEXT,
                                    level TEXT) diststyle all
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table(
                                    song_id TEXT PRIMARY KEY SORTKEY,
                                    title TEXT,
                                    artist_id TEXT DISTKEY,
                                    year INTEGER,
                                    duration FLOAT4) diststyle key
                          """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (
                                    artist_id TEXT PRIMARY KEY SORTKEY,
                                    name TEXT,
                                    location TEXT,
                                    latitude NUMERIC,
                                    longitude NUMERIC) diststyle all
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table (
                                    start_time TIMESTAMP PRIMARY KEY,
                                    hour INTEGER,
                                    day INTEGER,
                                    week INTEGER,
                                    month INTEGER,
                                    year INTEGER SORTKEY,
                                    weekday INTEGER)
                        """)

# STAGING TABLES
staging_events_copy = ("""COPY staging_events 
                            FROM {}
                            iam_role {}
                            JSON {}
                            region {}
                      """).format(config['S3']['LOG_DATA'], 
                                 config['IAM_ROLE']['ARN'], 
                                 config['S3']['LOG_JSONPATH'], 
                                 config['CLUSTER_INFO']['DWH_REGION'])

staging_songs_copy = ("""COPY staging_songs 
                            FROM {}
                            iam_role {}
                            JSON 'auto'
                            region {}
                      """).format(config.get('S3','SONG_DATA'), 
                                 config.get('IAM_ROLE', 'ARN'),  
                                 config.get('CLUSTER_INFO', 'DWH_REGION'))
# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay_table (
                                start_time, 
                                user_id, 
                                level, 
                                song_id, 
                                artist_id, 
                                session_id, 
                                location, 
                                user_agent) 
                            SELECT 
                                TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time, 
                                se.userId, 
                                se.level, 
                                ss.song_id, 
                                ss.artist_id, 
                                se.sessionid, 
                                se.location, 
                                se.userAgent
                            FROM staging_events se
                            LEFT JOIN staging_songs SS ON
                                (se.artist = ss.artist_name)  AND
                                (se.song = ss.title) AND
                                (se.length = ss.duration)
                            WHERE se.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO user_table (
                                        user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
                        SELECT DISTINCT
                                        userId,
                                        firstName,
                                        lastName,
                                        gender,
                                        level
                        FROM staging_events 
                        WHERE userId IS NOT NULL
""")

song_table_insert = ("""INSERT INTO song_table (
                                        song_id, 
                                        title, 
                                        artist_id, 
                                        year, 
                                        duration)
                        SELECT DISTINCT 
                                        song_id, 
                                        title, 
                                        artist_id, 
                                        year, 
                                        duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artist_table (
                                        artist_id, 
                                        name, 
                                        location, 
                                        latitude, 
                                        longitude)
                          SELECT DISTINCT
                                        artist_id, 
                                        artist_name,
                                        artist_location,
                                        artist_latitude,  
                                        artist_longitude
                         FROM staging_songs
                         WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time_table(
                                        start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
                        SELECT DISTINCT
                                        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                                        EXTRACT(hour FROM start_time),
                                        EXTRACT(day FROM start_time),
                                        EXTRACT(week FROM start_time),
                                        EXTRACT(month FROM start_time),
                                        EXTRACT(year FROM start_time),
                                        EXTRACT(weekday FROM start_time)
                        FROM staging_events  
                        WHERE ts IS NOT NULL
""")




# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
