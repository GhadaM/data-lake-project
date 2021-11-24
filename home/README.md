# Project: Data Lake

When dealing with a huge amount of data, time and resources are most valuable, and while AWS offers Amazon EMR which is a platform for rapidly processing, analyzing, and applying machine learning (ML) to big data using open-source frameworks. To have the extract and load operations work successfully, AWS offers also S3 where we can store and protect any amount of data. 
In this project we are extracting Data from S3 bucket, transforming it using the start schema, and loading it to a new S3 bucket.
Sparkify deals with a big amount of data and these technologies will offer high-speed data querying, analysis, and transformation.

## Schema

##### Source Tables

1. log_data
- contains app activity logs
   * artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId       
2. song_data 
- contains metadata about songs including the artist info
   * artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year
   
##### Fact Table

3. songplays
- records in event data associated with song plays i.e. records with page NextSong
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent  
##### Dimension Tables
    
4. users
- users in the app
    * user_id, first_name, last_name, gender, level
5. songs
- songs in music database
    * song_id, title, artist_id, year, duration
6. artists
- artists in music database
    * artist_id, name, location, latitude, longitude
7. time
- timestamps of records in songplays broken down into specific units
    * start_time, hour, day, week, month, year, weekday
    
## ETL Pipeline
1. Extract source tables from udacity-dend S3 Bucket 
2. create the fact and dimension tables from source tables
3. Load data to sprakify-data-lake-project S3 Buclet 
