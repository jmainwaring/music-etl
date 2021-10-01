import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('../redshift.cfg')
ARN=config.get('IAM_ROLE', 'ARN')



# UDFs
primary_artist_udf = """
CREATE OR REPLACE FUNCTION f_primary_artist (artist_array VARCHAR)
  RETURNS VARCHAR
STABLE
AS $$
  return eval(artist_array)[0]
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION f_secondary_artist (artist_array VARCHAR)
  RETURNS VARCHAR
STABLE
AS $$
  if len(eval(artist_array)) > 1:
    return eval(artist_array)[1]
  else: 
    return None
$$ LANGUAGE plpythonu;
"""


# DROP TABLES
dim_country_drop = "DROP TABLE IF EXISTS dim_country;"
dim_chart_drop = "DROP TABLE IF EXISTS dim_chart;"
dim_descriptors_drop = "DROP TABLE IF EXISTS dim_descriptors;"
dim_song_drop = "DROP TABLE IF EXISTS dim_song;"
fct_chart_movement_drop = "DROP TABLE IF EXISTS fct_chart_movement;"
dim_date_drop = "DROP TABLE IF EXISTS dim_date;"
dim_artist_drop = "DROP TABLE IF EXISTS dim_artist;"
dim_indiv_influence_drop = "DROP TABLE IF EXISTS dim_indiv_influence;"
dim_agg_influence_drop = "DROP TABLE IF EXISTS dim_agg_influence;"



# CREATE TABLES
dim_country_create = ("""
CREATE TABLE IF NOT EXISTS dim_country (
    id_country INT IDENTITY(100,1) PRIMARY KEY
  , name VARCHAR
);
""")


dim_chart_create = ("""
CREATE TABLE IF NOT EXISTS dim_chart (
    id_chart INT IDENTITY(100,1) PRIMARY KEY
  , name VARCHAR
);
""")

dim_descriptors_create = ("""
CREATE TABLE IF NOT EXISTS dim_descriptors (
    id_song VARCHAR PRIMARY KEY
  , descriptor_list VARCHAR
  , genre VARCHAR
);
""")

dim_song_create = ("""
CREATE TABLE IF NOT EXISTS dim_song (
    id_song VARCHAR PRIMARY KEY
  , title VARCHAR
  , id_artists VARCHAR
  , id_album VARCHAR
  , release_date DATE
  , explicit BOOLEAN
  , duration_ms INT
  , tempo INT
  , key INT
  , time_signature DECIMAL
  , danceability FLOAT
  , energy FLOAT
  , loudness FLOAT
  , speechiness FLOAT
  , acousticness FLOAT
  , instrumentalness FLOAT
  , liveness FLOAT
  , valence FLOAT
);
""")

fct_chart_movement_create = ("""
CREATE TABLE IF NOT EXISTS fct_chart_movement (
    id_movement INT IDENTITY(100,1) PRIMARY KEY
  , id_song VARCHAR
  , id_artists VARCHAR
  , id_primary_artist VARCHAR
  , id_secondary_artist VARCHAR
  , id_chart INT
  , current_rank INT
  , movement VARCHAR
  , id_country INT 
	, ds DATE
);
""")

dim_date_create = ("""
CREATE TABLE IF NOT EXISTS dim_date (
    ds DATE PRIMARY KEY
  , day INT
  , day_of_week INT
  , weekday_name VARCHAR
  , week INT
  , month INT
  , year INT
);
""")

dim_artist_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist (
    id_artist VARCHAR PRIMARY KEY
  , name VARCHAR
  , genres VARCHAR
);
""")

dim_indiv_influence_create = ("""
CREATE TABLE IF NOT EXISTS dim_indiv_influence (
    id_influence INT IDENTITY(100,1) PRIMARY KEY
  , follower_name VARCHAR
  , follower_main_genre VARCHAR
  , follower_active_start INT
  , influencer_name VARCHAR
  , influencer_main_genre VARCHAR
  , influencer_active_start INT
);
""")

dim_agg_influence_create = ("""
CREATE TABLE IF NOT EXISTS dim_agg_influence (
    influencer_id INT PRIMARY KEY
  , influencer_name VARCHAR
  , depth_0 INT
  , depth_1 INT
  , depth_2 INT
  , depth_3 INT
  , depth_4 INT
  , depth_5 INT
  , depth_6 INT
  , depth_7 INT
  , depth_8 INT
  , depth_9 INT
  , depth_10 INT
  , total_scaled FLOAT
);
""")


                              
# INSERT RECORDS
dim_country_insert = """
INSERT INTO dim_country (name)
SELECT DISTINCT region
FROM staging_charts
ORDER BY region
""".format('dim_country', ARN)




dim_chart_insert = """
INSERT INTO dim_chart (name)
SELECT DISTINCT chart
FROM staging_charts
ORDER BY chart;
""".format('dim_chart', ARN)


dim_descriptors_insert = """
INSERT INTO dim_descriptors
SELECT 
	  spotify_id
	, seeds 
	, genre
FROM staging_song_adjectives;
""".format('dim_descriptors', ARN)


dim_song_insert = """
INSERT INTO dim_song

WITH duplicate_songs AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY name, artist_ids, release_date ORDER BY id) AS row_num
	FROM staging_songs_full
)

SELECT
	  id 							
  , name						
  , artist_ids										
  , album_id					
  , CAST(release_date AS DATE)			
  , CASE WHEN explicit = 'True' then TRUE else FALSE END
  , CAST(duration_ms AS INT)
  , ROUND(CAST(tempo AS FLOAT), 0)
  , CAST(key AS INT)
  , ROUND(CAST(time_signature AS FLOAT), 1)
  , ROUND(CAST(danceability AS FLOAT), 2)
  , ROUND(CAST(energy AS FLOAT), 2)
  , ROUND(CAST(loudness AS FLOAT), 2)
  , ROUND(CAST(speechiness AS FLOAT), 2)
  , ROUND(CAST(acousticness AS FLOAT), 2)
  , ROUND(CAST(instrumentalness AS FLOAT), 2)
  , ROUND(CAST(liveness AS FLOAT), 2)
  , ROUND(CAST(valence AS FLOAT), 2)
FROM duplicate_songs
WHERE 
    release_date ~ '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
	AND row_num = 1;
""".format('dim_song', ARN)


fct_chart_movement_insert = """
INSERT INTO fct_chart_movement (id_song, id_artists, id_primary_artist, id_secondary_artist, id_chart, current_rank, movement, id_country, ds)
SELECT
  ss.id								
, ss.artist_ids				
, f_primary_artist(artist_ids)	
, f_secondary_artist(artist_ids)		
, CAST (dch.id_chart AS INT)					
, CAST (sc.rank AS INT)							
, sc.trend							
, CAST (dc.id_country AS INT)					
, CAST (sc.date AS DATE)							
FROM staging_charts sc
JOIN staging_songs_full ss 
    ON LOWER(sc.title) = LOWER(ss.name)
    AND REGEXP_REPLACE(sc.artists, '[^a-zA-Z0-9]+', '') = REGEXP_REPLACE(ss.artists, '[^a-zA-Z0-9]+', '')
JOIN dim_country dc 
	ON sc.region = dc.name
JOIN dim_chart dch 
	ON sc.chart = dch.name
JOIN dim_song ds
	ON ss.id = ds.id_song
WHERE 
	sc.rank ~ '[0-9]+'
    AND ds.release_date ~ '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
	AND LENGTH(artist_ids) < 150;
""".format('fct_chart_movement', ARN)


dim_date_insert = """
INSERT INTO dim_date

WITH distinct_ds AS (
	SELECT DISTINCT ds 
	FROM fct_chart_movement
	ORDER BY ds
)

SELECT
	  ds
	, EXTRACT(DAY FROM ds)
	, EXTRACT(DOW FROM ds)
	, to_char(ds, 'Day')
	, EXTRACT(WEEK FROM ds)
	, EXTRACT(MONTH FROM ds)
	, EXTRACT(YEAR FROM ds)
FROM distinct_ds;
""".format('dim_date', ARN)


dim_artist_insert = """
INSERT INTO dim_artist

WITH distinct_ids AS (
	SELECT DISTINCT id_primary_artist
	FROM fct_chart_movement
)

SELECT 
	  TRANSLATE(spotify_uri, 'spotify:artist:', '') AS id_artist
	, name
	, genres
FROM staging_artist_id_mapping saim  
JOIN distinct_ids di
	ON TRANSLATE(saim.spotify_uri, 'spotify:artist:', '') = di.id_primary_artist;
""".format('dim_artist', ARN)


dim_indiv_influence_insert = """
INSERT INTO dim_indiv_influence (follower_name, follower_main_genre, follower_active_start, influencer_name, influencer_main_genre, influencer_active_start)

SELECT 
	  follower_name
	, follower_main_genre
	, CAST(follower_active_start AS INT)
	, influencer_name
	, influencer_main_genre
	, CAST(influencer_active_start AS INT)
FROM staging_all_influences sai 
JOIN dim_artist da
	ON CASE 
        WHEN sai.follower_name = da.name THEN 1
        WHEN sai.influencer_name = da.name THEN 1
        ELSE 0 END = 1;
""".format('dim_indiv_influence', ARN)


dim_agg_influence_insert = """
INSERT INTO dim_agg_influence 

WITH duplicate_df AS (
SELECT 
	  CAST (influencer_id AS INT)
	, influencer_name
	, CAST (depth_0 AS INT)
	, CAST (depth_1 AS INT)
	, CAST (depth_2 AS INT)
	, CAST (depth_3 AS INT)
	, CAST (depth_4 AS INT)
	, CAST (depth_5 AS INT)
	, CAST (depth_6 AS INT)
	, CAST (depth_7 AS INT)
	, CAST (depth_8 AS INT)
	, CAST (depth_9 AS INT)
	, CAST (depth_10 AS INT) 
	, CAST (total AS FLOAT)
	, ROW_NUMBER() OVER (PARTITION BY influencer_name ORDER BY influencer_id) AS row_num
FROM staging_influence_depth sid 
JOIN dim_artist da
	ON sid.influencer_name = da.name
)

SELECT
	  influencer_id
	, influencer_name
	, depth_0
	, depth_1
	, depth_2
	, depth_3
	, depth_4
	, depth_5
	, depth_6
	, depth_7
	, depth_8
	, depth_9
	, depth_10
	, total
FROM duplicate_df
WHERE 
	row_num = 1;
""".format('dim_agg_influence', ARN)



# QUERY LISTS
udf_queries = [primary_artist_udf]
drop_table_queries = [dim_country_drop, dim_chart_drop, dim_descriptors_drop, dim_song_drop, fct_chart_movement_drop, dim_date_drop, dim_artist_drop, dim_indiv_influence_drop, dim_agg_influence_drop]
create_table_queries = [dim_country_create, dim_chart_create, dim_descriptors_create, dim_song_create, fct_chart_movement_create, dim_date_create, dim_artist_create, dim_indiv_influence_create, dim_agg_influence_create]
insert_records_queries = [dim_country_insert, dim_chart_insert, dim_descriptors_insert, dim_song_insert, fct_chart_movement_insert,  dim_date_insert, dim_artist_insert, dim_indiv_influence_insert, dim_agg_influence_insert]