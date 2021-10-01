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


-- Like above, final table won't be VARCHAR but staging column had some records with letters
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


-- Like above, final table won't be VARCHAR but staging column had some records with letters
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


-- !! like above, final table won't be VARCHAR but staging column had some records with letters
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


-- !! like above, final table won't be VARCHAR but staging column had some records with letters
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


-- !! like above, final table won't be VARCHAR but staging column had some records with letters
CREATE TABLE IF NOT EXISTS staging_artist_id_mapping (
      row_num VARCHAR
    , name VARCHAR
    , genres VARCHAR
    , spotify_uri VARCHAR
);

