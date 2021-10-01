------------------------------------------------------------------------------
SELECT fcm.id_song, COUNT(*), title, ds.id_artists
FROM fct_chart_movement fcm
JOIN dim_song ds 
    ON fcm.id_song = ds.id_song
WHERE id_chart = 102
GROUP BY fcm.id_song, ds.title, ds.id_artists
ORDER by count(*) DESC
LIMIT 5;



------------------------------------------------------------------------------
DROP TABLE IF EXISTS pop_attr_comparison;

CREATE TABLE pop_attr_comparison AS

SELECT 
      leader_country_id
    , leader_country_name
    , COUNT(*)                       AS song_leadership_count   
    , SUM(days_between)              AS total_days_between
    , AVG(days_between)              AS avg_days_between
FROM first_chart_appearance
GROUP BY leader_country_id, leader_country_name  
ORDER BY COUNT(*) DESC
LIMIT 10;
WITH top_song_ids AS (
    SELECT DISTINCT fcm.id_song
    FROM fct_chart_movement fcm
    JOIN dim_song ds 
        ON fcm.id_song = ds.id_song
    WHERE id_chart = 102
    GROUP BY fcm.id_song, ds.title, ds.id_artists
    ORDER by COUNT(*) DESC
    LIMIT 1000
)

SELECT 
      'TRUE' AS is_top_songs
    , AVG(duration_ms)                                          AS avg_duration
    , AVG(tempo)                                                AS avg_tempo
    , ROUND(SUM(CAST(explicit AS INT))/(COUNT(*) * 1.0), 3)     AS explicit_pct
    , ROUND(AVG(danceability), 3)                               AS avg_danceability
    , ROUND(AVG(energy), 3)                                     AS avg_energy
    , ROUND(AVG(loudness), 3)                                   AS avg_loudness
    , ROUND(AVG(speechiness), 3)                                AS avg_speechiness
    , ROUND(AVG(acousticness), 3)                               AS avg_acousticness  
    , ROUND(AVG(instrumentalness), 3)                           AS avg_instrumentalness          
    , ROUND(AVG(liveness), 3)                                   AS avg_liveness
    , ROUND(AVG(valence), 3)                                    AS avg_valence
FROM top_song_ids tsi
JOIN dim_song ds
    ON tsi.id_song = ds.id_song

UNION

SELECT 
     'FALSE' AS is_top_songs
    , AVG(duration_ms)
    , AVG(tempo)
    , ROUND(SUM(CAST(explicit AS INT))/(COUNT(*) * 1.0), 3)
    , ROUND(AVG(danceability), 3)
    , ROUND(AVG(energy), 3)
    , ROUND(AVG(loudness), 3)
    , ROUND(AVG(speechiness), 3)
    , ROUND(AVG(acousticness), 3)
    , ROUND(AVG(instrumentalness), 3)
    , ROUND(AVG(liveness), 3)
    , ROUND(AVG(valence), 3)
FROM dim_song;



----------------------------------------------------------------------

-- Examining characteristics of hits by country
DROP TABLE IF EXISTS pop_attr_countries;

CREATE TABLE pop_attr_countries AS

WITH top_songs AS (
    SELECT id_country, fcm.id_song, COUNT(*)
    FROM fct_chart_movement fcm
    JOIN dim_song ds 
        ON fcm.id_song = ds.id_song
    WHERE id_chart = 102
    GROUP BY id_country, fcm.id_song
    HAVING COUNT(*) > 10
    ORDER BY id_country, COUNT(*) DESC
)

SELECT 
      dc.id_country
    , dc.name
    , AVG(duration_ms)                                          AS avg_duration
    , AVG(tempo)                                                AS avg_tempo
    , ROUND(SUM(CAST(explicit AS INT))/(COUNT(*) * 1.0), 3)     AS explicit_pct
    , ROUND(AVG(danceability), 3)                               AS avg_danceability
    , ROUND(AVG(energy), 3)                                     AS avg_energy
    , ROUND(AVG(loudness), 3)                                   AS avg_loudness
    , ROUND(AVG(speechiness), 3)                                AS avg_speechiness
    , ROUND(AVG(acousticness), 3)                               AS avg_acousticness  
    , ROUND(AVG(instrumentalness), 3)                           AS avg_instrumentalness          
    , ROUND(AVG(liveness), 3)                                   AS avg_liveness
    , ROUND(AVG(valence), 3)                                    AS avg_valence
FROM top_songs ts
JOIN dim_song ds
    ON ts.id_song = ds.id_song 
JOIN dim_country dc
    ON ts.id_country = dc.id_country
GROUP BY dc.id_country, dc.name;




WITH top_songs AS (
    SELECT id_country, fcm.id_song, COUNT(*)
    FROM fct_chart_movement fcm
    JOIN dim_song ds 
        ON fcm.id_song = ds.id_song
    WHERE id_chart = 102
    GROUP BY id_country, fcm.id_song
    HAVING COUNT(*) > 10
    ORDER BY id_country, COUNT(*) DESC
)

SELECT 
      id_country
    , dc.name
    , AVG(duration_ms)                                          AS avg_duration
    , AVG(tempo)                                                AS avg_tempo
    , ROUND(SUM(CAST(explicit AS INT))/(COUNT(*) * 1.0), 3)     AS explicit_pct
    , ROUND(AVG(danceability), 3)                               AS avg_danceability
    , ROUND(AVG(energy), 3)                                     AS avg_energy
    , ROUND(AVG(loudness), 3)                                   AS avg_loudness
    , ROUND(AVG(speechiness), 3)                                AS avg_speechiness
    , ROUND(AVG(acousticness), 3)                               AS avg_acousticness  
    , ROUND(AVG(instrumentalness), 3)                           AS avg_instrumentalness          
    , ROUND(AVG(liveness), 3)                                   AS avg_liveness
    , ROUND(AVG(valence), 3)                                    AS avg_valence
FROM top_songs ts
JOIN dim_song ds
    ON ts.id_song = ds.id_song 
JOIN dim_country dc
    ON ts.id_country = dc.id_country
GROUP BY id_country, dc.name
ORDER BY id_country
LIMIT 12;


----------------------------------------------------------------------

-- Countries that are leading the trends
DROP TABLE IF EXISTS first_chart_appearance;

CREATE TABLE first_chart_appearance AS

WITH first_appearance AS (

SELECT 
      id_song
    , id_country
    , MIN(ds)                            AS ds
FROM fct_chart_movement
WHERE id_chart = 102 
GROUP BY id_song, id_country  
) 

SELECT 
      fa1.id_song
    , fa1.id_country                    AS leader_country_id
    , dc1.name                          AS leader_country_name
    , fa1.ds                            AS leader_ds
    , fa2.id_country                    AS follower_country_id
    , dc2.name                          AS follower_country_name
    , fa2.ds                            AS follower_ds
    , DATEDIFF(day, fa1.ds, fa2.ds)     AS days_between
FROM first_appearance fa1
JOIN first_appearance fa2
    ON fa1.id_song = fa2.id_song
JOIN dim_country dc1 
    ON fa1.id_country = dc1.id_country
JOIN dim_country dc2 
    ON fa2.id_country = dc2.id_country
    AND fa1.ds < fa2.ds
WHERE dc1.name != 'Global';

SELECT *
FROM first_chart_appearance
LIMIT 10;



DROP TABLE IF EXISTS ten_leading_countries;

CREATE TABLE ten_leading_countries AS

SELECT 
      leader_country_id
    , leader_country_name
    , COUNT(*)                       AS song_leadership_count   
    , SUM(days_between)              AS total_days_between
    , AVG(days_between)              AS avg_days_between
FROM first_chart_appearance
GROUP BY leader_country_id, leader_country_name  
ORDER BY COUNT(*) DESC
LIMIT 10;


DROP TABLE IF EXISTS ten_following_countries;

CREATE TABLE ten_following_countries AS

SELECT 
      leader_country_id
    , leader_country_name
    , COUNT(*)                       AS song_leadership_count   
    , SUM(days_between)              AS total_days_between
    , AVG(days_between)              AS avg_days_between
FROM first_chart_appearance
GROUP BY leader_country_id, leader_country_name  
ORDER BY song_leadership_count  
LIMIT 10;