import configparser
import datetime as dt
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("datalake_test_2") \
        .getOrCreate()
    print('spark session created')
    return spark

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data'
    
    # read song data file
    df = spark.read.json(song_data)
    print('song_data loaded')
    # extract columns to create songs table
    df.createOrReplaceTempView('stagingsongs_table')
    print('stagingsongs_table Temp table created')
    songs_table= spark.sql('''
                        SELECT song_id, 
                        title, 
                        artist_id, 
                        year, 
                        duration
                        FROM stagingsongs_table
                        ''')
    
    print('songs_table created')
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs')
    print('songs_table file created')
    
    # extract columns to create artists table
    artists_table = spark.sql('''
                SELECT artist_id, 
                artist_name        AS name, 
                artist_location    AS location, 
                artist_latitude    AS latitude, 
                artist_longitude   AS longitude
                FROM stagingsongs_table
                ''')
    print('artist table created')
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artist')
    print('artist table file created')
    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data'

    # read log data file
    df = spark.read.json(log_data)
    print('log_data loaded')
    
    # filter by actions for song plays
    df = df[df.page =='NextSong']
    print('NextSong data filtered')

    # extract columns for users table 
    df.createOrReplaceTempView('stagingevents_table')
    print('stagingevents_table temp table created')
    
    users_table = spark.sql('''
                        SELECT userId    AS user_id, 
                        firstName        AS first_name, 
                        lastName         AS last_name, 
                        gender, 
                        level
                        FROM stagingevents_table
                        ''')
    print('users table created')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')
    print('users table file created')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: dt.datetime.fromtimestamp(x / 1000), TimestampType())
    df_ts =df.withColumn("timestamp", get_timestamp(df.ts))
    print('timestamp column added')
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 


    # extract columns to create time table
    df_ts.createOrReplaceTempView('df_ts_table')
    print('df_ts_table temp table created')
    
    time_table = spark.sql('''
                        SELECT timestamp                     AS start_time, 
                        EXTRACT(hour      FROM timestamp)    AS hour, 
                        EXTRACT(day       FROM timestamp)    AS day, 
                        EXTRACT(week      FROM timestamp)    AS week, 
                        EXTRACT(month     FROM timestamp)    AS month, 
                        EXTRACT(year      FROM timestamp)    AS year, 
                        EXTRACT(dayofweek FROM timestamp)    AS weekday
                        FROM df_ts_table
                        ''')
    print('time table created')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time')
    print('time table file created')
    
    # read in song data to use for songplays table
    song_df = input_data + 'song_data'
    s_df = spark.read.json(song_df)
    print('song df data read in')
    
    s_df.createOrReplaceTempView('songs_table')
    print('songs_table temp table created')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                        SELECT monotonically_increasing_id() AS songplay_id, 
                        se.timestamp                         AS start_time, 
                        se.userId                            AS user_id, 
                        se.level, 
                        ss.song_id, 
                        ss.artist_id, 
                        se.sessionId                         AS session_id, 
                        se.location, 
                        se.userAgent                         AS user_agent,
                        EXTRACT(month FROM se.timestamp)     AS month,
                        EXTRACT(year FROM se.timestamp)      AS year
                        FROM df_ts_table as se
                        JOIN songs_table as ss
                        ON ss.artist_name = se.artist
                        ''')
    print('songplays table created')
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays')
    print('songplays table file created')
    
def main():
    spark = create_spark_session()
    input_data = "data/"
    output_data = "data/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
