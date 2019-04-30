import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS  songplays;"
user_table_drop = "DROP TABLE IF EXISTS  users;"
song_table_drop = "DROP TABLE IF EXISTS  songs;"
artist_table_drop = "DROP TABLE IF EXISTS  artists;"
time_table_drop = "DROP TABLE IF EXISTS  time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
	artist  TEXT NULL,
	auth  TEXT NULL,
	firstName  TEXT NULL,
	gender  TEXT NULL,
	itemInSession  TEXT NULL,
	lastName  TEXT NULL,
	length FLOAT NULL,
	level  TEXT NULL,
	location  TEXT NULL,
	method  TEXT NULL,
	page  TEXT NULL,
	registration FLOAT NULL,
	sessionId INT NULL,
	song  TEXT NULL,
	status INT NULL,
	ts timestamp NULL,
	userAgent  TEXT NULL,
	userId  TEXT NULL
	);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT NULL,
	artist_id  TEXT NULL,
	artist_latitude FLOAT NULL,
	artist_longitude FLOAT NULL,
	artist_location  TEXT NULL ,
    artist_name  TEXT NULL,
	song_id  TEXT NULL,
	title  TEXT NULL,
	duration FLOAT NULL,
	year INT NULL
	);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id  TEXT NOT NULL,
    level  TEXT NOT NULL,
    song_id  TEXT,
    artist_id  TEXT,
    session_id  TEXT NOT NULL,
    user_agent  TEXT NOT NULL,
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
    FOREIGN KEY (start_time) REFERENCES time (start_time),
    FOREIGN KEY (user_id) REFERENCES users (user_id)
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id  TEXT PRIMARY KEY,
    first_name  TEXT NOT NULL,
    last_name  TEXT NOT NULL,
    gender  TEXT NOT NULL,
    level  TEXT NOT NULL
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id  TEXT PRIMARY KEY,
    title  TEXT NOT NULL,
    artist_id  TEXT NOT NULL,
    year INT NOT NULL,
    duration FLOAT NOT NULL,
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    name  TEXT NOT NULL,
    artist_id  TEXT PRIMARY KEY,
    location  TEXT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
iam_role {}
region 'us-west-2'
format as JSON {}
TRUNCATECOLUMNS blanksasnull emptyasnull
TIMEFORMAT AS 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {}
iam_role {}
region 'us-west-2'
FORMAT as JSON 'auto'
TRUNCATECOLUMNS blanksasnull emptyasnull;
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    user_agent
    )
SELECT ts, userId, level, song_id, artist_id, sessionId, userAgent
FROM staging_events as e, staging_songs as s
WHERE 1=1 AND
    e.artist = s.artist_name AND
    e.song = s.title AND
    ts IS NOT NULL AND
    userId IS NOT NULL AND
    level IS NOT NULL AND
    song_id IS NOT NULL AND
    artist_id IS NOT NULL AND
    sessionId IS NOT NULL AND
    userAgent IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT
    se.userId,
    se.firstName,
    se.lastName,
    se.gender,
    se.level
FROM (SELECT 
        staging_events.userId,
        staging_events.firstName,
        staging_events.lastName,
        staging_events.gender,
        staging_events.level,
        ROW_NUMBER() OVER (PARTITION BY staging_events.userId
                            ORDER BY staging_events.ts DESC) AS rank
        FROM staging_events) as se
  	WHERE se.rank =1
        AND userId IS NOT NULL
        AND firstName IS NOT NULL
        AND lastName IS NOT NULL
        AND gender IS NOT NULL
        AND level IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE 1=1
    AND song_id IS NOT NULL
    AND title IS NOT NULL
    AND artist_id IS NOT NULL
    AND year IS NOT NULL
    AND duration IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT artist_id, artist_name, artist_latitude, artist_longitude
FROM staging_songs
WHERE 1=1
    AND artist_id IS NOT NULL
    AND artist_name IS NOT NULL
    AND artist_latitude IS NOT NULL
    AND artist_longitude IS NOT NULL
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
SELECT ts,
    extract(h from ts),
    extract(d from ts),
    extract(w from ts),
    extract(mon from ts),
    extract(y from ts),
    extract(weekday from ts)
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create,
                        time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
