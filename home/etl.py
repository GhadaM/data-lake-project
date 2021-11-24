import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    creating a SparkSession instance spark

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .appName("Sparkify Data Lake") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """ 
    
    process_song_data loads song data from the S3 bucket udacity-dend to a Spark dataframe
    then create two dimensions tables  from the data frame: songs and artists 
    and saves them as Parquet files in a new S3 bucket
    the table songs is partitioned by artist and year 
    
   :param spark: spark instance
   :param input_data: the path to the source S3 bucket
   :param output_data: the path to the S3 bucket where the tables will be stored as parquet files
   
    """
        
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", 
                            "title", 
                            "artist_id", 
                            "year", 
                            "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_path = output_data + 'songs/songs.parquet'
    songs_table.write.partitionBy('year', 'artist_id') \
                 .mode('overwrite') \
                 .parquet(songs_path)

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", 
                                  "artist_name as name", 
                                  "artist_location as location", 
                                  "artist_latitude as latitude", 
                                  "artist_longitude as longitude") \
                      .dropDuplicates(["artist_id"])

    
    # write artists table to parquet files
    artist_path  = output_data + 'artists/artists.parquet'
    artists_table.write.mode('overwrite') \
                   .parquet(artist_path)


def process_log_data(spark, input_data, output_data):
    """
    
    process_log_data loads log data from the S3 bucket udacity-dend to a Spark dataframe
    then create two dimensions tables  from the data frame: users and time 
    and fact table songplays 
    and store them as Parquet files in a new S3 bucket
    the tables time and songplays are partitioned by year and month 
    for the creation of songplays the song data is also loaded 
    and then a join query is performed 
    
    :param spark: spark instance
    :param input_data: the path to the source S3 bucket
    :param output_data: the path to the S3 bucket where the tables will be stored as parquet files
   
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", 
                                "firstName as first_name", 
                                "lastName as last_name", 
                                "gender", 
                                "level") \
                    .dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_path  = output_data + 'users/users.parquet'
    users_table.write.mode('overwrite') \
                 .parquet(users_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : (x/1000))
    df = df.withColumn("ts_timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x),  TimestampType())
    df = df.withColumn("start_time", get_datetime(df.ts_timestamp))
    
    # extract columns to create time table
    time_table = df.select('start_time')\
                   .withColumn("hour", hour(df.start_time)) \
                   .withColumn("day", dayofmonth(df.start_time)) \
                   .withColumn("week", weekofyear(df.start_time)) \
                   .withColumn("month", month(df.start_time)) \
                   .withColumn("year", year(df.start_time)) \
                   .withColumn("weekday", dayofweek(df.start_time)) \
                   .dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_path = output_data + 'time/time.parquet'
    time_table.write.partitionBy('year', 'month') \
              .mode('overwrite') \
              .parquet(time_path)

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, \
                              (song_df.artist_name == df.artist)
                              & (song_df.title == df.song) 
                              & (song_df.duration == df.length )) \
                        .select("start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent") \
                        .withColumn("songplay_id", monotonically_increasing_id()) \
                        .withColumn("month", month(df.start_time)) \
                        .withColumn("year", year(df.start_time)) 

    # write songplays table to parquet files partitioned by year and month
    songplays_path = output_data + 'songplays/songplays.parquet'
    songplays_table.write.partitionBy('year', 'month') \
                 .mode('overwrite') \
                 .parquet(songplays_path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sprakify-data-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
