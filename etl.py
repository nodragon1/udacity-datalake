import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = 's3a://udacity-dend/song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, 'song_parquet'), mode = 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', col('artist_name').alias('name'), \
                              col('artist_location').alias('location'), \
                              col('artist_latitude').alias('latitude'), \
                              col('artist_longitude').alias('longitude'))
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artist_parquet'), mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = 's3a://udacity-dend/log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'), \
                            col('firstName').alias('first_name'), \
                            col('lastName').alias('last_name'), \
                            'gender', 'level')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'user_parquet'), mode = 'overwrite')

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract hour, day, month etc and add informations with new columns
    df = df.withColumn('year', year(df.datetime)) \
           .withColumn('month', month(df.datetime)) \
           .withColumn('day', dayofmonth(df.datetime)) \
           .withColumn('hour', hour(df.datetime)) \
           .withColumn('week', weekofyear(df.datetime)) \
           .withColumn('weekday', dayofweek(df.datetime))
        
    # extract columns to create time table
    time_table = df.select(col('ts').alias('start_time'), 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, 'time_parquet'), mode = 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json('s3a://udacity-dend/song_data/A/A/A/*.json')
    
    song_df.createOrReplaceTempView("song_table")
    df.createOrReplaceTempView("log_table")
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                                SELECT row_number() over (order by s.artist_id) as songplay_id, \
                                       l.ts start_time, l.userId user_id, l.level, s.song_id, \
                                       s.artist_id, l.sessionId session_id, s.artist_location location, \
                                       l.userAgent user_agent, year(l.datetime) year, month(l.datetime) month
                                FROM song_table s JOIN log_table l ON (s.title = l.song AND s.duration = l.length
                                AND s.artist_name = l.artist)
                                ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, 'songplay_parquet'), mode = 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
