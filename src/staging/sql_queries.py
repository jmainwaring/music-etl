import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('../redshift.cfg')
ARN=config.get('IAM_ROLE', 'ARN')
SPOTIFY_DATA=config.get('S3', 'SPOTIFY_DATA')


# DROP TABLES
staging_charts_drop = "DROP TABLE IF EXISTS staging_charts;"
staging_songs_full_drop = "DROP TABLE IF EXISTS staging_songs_full;"
staging_song_adjectives_drop = "DROP TABLE IF EXISTS staging_song_adjectives;"
staging_all_influences_drop = "DROP TABLE IF EXISTS staging_all_influences;"
staging_influence_depth_drop = "DROP TABLE IF EXISTS staging_influence_depth;"
staging_artist_id_mapping_drop = "DROP TABLE IF EXISTS staging_artist_id_mapping;"


# CREATE TABLES
staging_charts_create = ("""
CREATE TABLE IF NOT EXISTS staging_charts (
  title VARCHAR NOT NULL
, rank VARCHAR
, date VARCHAR
, artists VARCHAR
, url VARCHAR
, region VARCHAR
, chart VARCHAR
, trend VARCHAR 
, streams VARCHAR 
);
""")

staging_songs_full_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_full (
      id VARCHAR
    , name VARCHAR
    , album VARCHAR
    , album_id VARCHAR
    , artists VARCHAR
    , artist_ids VARCHAR
    , track_number VARCHAR 
    , disc_number VARCHAR  
    , explicit VARCHAR
    , danceability VARCHAR
    , energy VARCHAR
    , key VARCHAR
    , loudness VARCHAR
    , mode VARCHAR
    , speechiness VARCHAR
    , acousticness VARCHAR
    , instrumentalness  VARCHAR
    , liveness VARCHAR
    , valence VARCHAR
    , tempo VARCHAR
    , duration_ms VARCHAR
    , time_signature VARCHAR
    , year VARCHAR
    , release_date VARCHAR
);
""")

staging_song_adjectives_create = ("""
CREATE TABLE IF NOT EXISTS staging_song_adjectives (
      lastfm_url VARCHAR
    , track VARCHAR
    , artist VARCHAR
    , seeds VARCHAR
    , number_of_emotion_tags VARCHAR
    , valence_tags VARCHAR
    , arousal_tags VARCHAR
    , dominance_tags VARCHAR
    , mbid VARCHAR
    , spotify_id VARCHAR
    , genre VARCHAR
);
""")

staging_all_influences_create = ("""
CREATE TABLE IF NOT EXISTS staging_all_influences (
      influencer_id VARCHAR
    , influencer_name VARCHAR
    , influencer_main_genre VARCHAR
    , influencer_active_start VARCHAR
    , follower_id VARCHAR
    , follower_name VARCHAR
    , follower_main_genre VARCHAR
    , follower_active_start VARCHAR
);
""")

staging_influence_depth_create = ("""
CREATE TABLE IF NOT EXISTS staging_influence_depth (
      influencer_id VARCHAR
    , influencer_name VARCHAR
    , depth_0 VARCHAR
    , depth_1 VARCHAR
    , depth_2 VARCHAR
    , depth_3 VARCHAR
    , depth_4 VARCHAR
    , depth_5 VARCHAR
    , depth_6 VARCHAR
    , depth_7 VARCHAR
    , depth_8 VARCHAR
    , depth_9 VARCHAR
    , depth_10 VARCHAR
    , total VARCHAR
);
""")

staging_artist_id_mapping_create = ("""
CREATE TABLE IF NOT EXISTS staging_artist_id_mapping (
      row_num VARCHAR
    , name VARCHAR
    , genres VARCHAR
    , spotify_uri VARCHAR
);
""")


                              
# COPY TABLES
staging_charts_copy = """
COPY {} FROM 's3://spotify-dataeng-nano/top_charts.csv' TRUNCATECOLUMNS
IAM_ROLE {}
CSV
IGNOREHEADER 1
""".format('staging_charts', ARN)

staging_songs_full_copy = """
COPY {} FROM 's3://spotify-dataeng-nano/1m_songs_features.csv' TRUNCATECOLUMNS
IAM_ROLE {}
CSV
IGNOREHEADER 1
""".format('staging_songs_full', ARN)

staging_song_adjectives_copy = """
COPY {} FROM 's3://spotify-dataeng-nano/90000_song_adjectives.csv' TRUNCATECOLUMNS
IAM_ROLE {}
CSV
IGNOREHEADER 1
""".format('staging_song_adjectives', ARN)

staging_all_influences_copy = """
COPY {} FROM 's3://spotify-dataeng-nano/artist_musical_influences/all_influences.csv' TRUNCATECOLUMNS
IAM_ROLE {}
CSV
""".format('staging_all_influences', ARN)

staging_influence_depth_copy = """
COPY {} FROM 's3://spotify-dataeng-nano/artist_musical_influences/influence_depth.csv' TRUNCATECOLUMNS
IAM_ROLE {}
CSV
""".format('staging_influence_depth', ARN)

staging_artist_id_mapping_copy = """
COPY {} FROM 's3://spotify-dataeng-nano/66m_artist_uri_mapping.csv' TRUNCATECOLUMNS
IAM_ROLE {}
CSV
IGNOREHEADER 1
""".format('staging_artist_id_mapping', ARN)



# QUERY LISTS
drop_table_queries = [staging_charts_drop, staging_songs_full_drop, staging_song_adjectives_drop, staging_all_influences_drop, staging_influence_depth_drop, staging_artist_id_mapping_drop]
create_table_queries = [staging_charts_create, staging_songs_full_create, staging_song_adjectives_create, staging_all_influences_create, staging_influence_depth_create, staging_artist_id_mapping_create]
copy_table_queries = [staging_charts_copy, staging_songs_full_copy, staging_song_adjectives_copy, staging_all_influences_copy, staging_influence_depth_copy, staging_artist_id_mapping_copy]