CREATE TABLE IF NOT EXISTS public.songs (
	songid VARCHAR NOT NULL,
	title VARCHAR,
	artistid VARCHAR,
	"year" INT,
	duration NUMERIC,
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);



CREATE TABLE IF NOT EXISTS staging_charts (
  title VARCHAR NOT NULL
, rank VARCHAR -- Should be int but had letters in there
, date VARCHAR -- Should be date but had invalid characters
, artists VARCHAR
, url VARCHAR
, region VARCHAR
, chart VARCHAR
, trend VARCHAR 
, streams VARCHAR -- Should be int but had letters in there
)





CREATE TABLE IF NOT EXISTS public.songs (
	songid VARCHAR NOT NULL,
	title VARCHAR,
	artistid VARCHAR,
	"year" INT,
	duration NUMERIC,
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);


CREATE TABLE IF NOT EXISTS public.staging_events (
	artist VARCHAR,
	status INT,
	ts INT8,
	useragent VARCHAR,
	userid INT
);


CREATE TABLE IF NOT EXISTS public.staging_songs (
	num_songs INT,
	artist_id VARCHAR,
	artist_name VARCHAR,
	artist_latitude NUMERIC,
	artist_longitude NUMERIC,
	artist_location VARCHAR,
	song_id VARCHAR,
	title VARCHAR,
	duration NUMERIC,
	"year" INT
);


CREATE TABLE IF NOT EXISTS public.users (
	userid INT NOT NULL,
	first_name VARCHAR,
	last_name VARCHAR,
	gender VARCHAR,
	"level" VARCHAR,
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);
