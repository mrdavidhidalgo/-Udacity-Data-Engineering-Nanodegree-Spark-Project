import configparser
from datetime import datetime
import os
from uuid import uuid4
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, max as sql_max, desc
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import StructType as stt, StructField as r, IntegerType as IT, LongType as LT, StringType as ST,DoubleType as DT


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']= config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']= config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """
      Runs the ETL process for the songs folder. It saves the result into parquets tables (songs and artists) .
     """


    song_schema = stt([
    r('num_songs', LT(), True),
    r('artist_id', ST(), True),
    r('artist_location', ST(), True),
    r('artist_latitude', DT(), True),
    r('artist_longitude', DT(), True),
    r('artist_name', ST(), True),
    r('song_id', ST(), True),
    r('title', ST(), True),
    r('duration', DT(), True),
    r('year', IT(), True)])
    
    songs_path = input_data + "song_data/*/*/*/*"
    
    df = spark.read.json(songs_path, schema = song_schema)

    songs_table = df.filter(df["song_id"].isNotNull()) \
    .select(df["song_id"], df["title"], df["artist_id"], df["duration"], df["year"]) \
    .distinct()
    
    songs_table.write \
           .partitionBy("year", "artist_id") \
           .parquet(output_data + "songs")

    artists_table = df.filter(df["artist_id"].isNotNull()) \
                .select(df["artist_id"], df["artist_name"], \
                        df["artist_location"], df["artist_latitude"], df["artist_longitude"]) \
                .distinct() \
                .withColumnRenamed("artist_name", "name") \
                .withColumnRenamed("artist_location", "location") \
                .withColumnRenamed("artist_latitude", "lattitude") \
                .withColumnRenamed("artist_longitude", "longitude")

    artists_table.write.parquet(output_data + "artists")

def process_log_data(spark, input_data, output_data):
    """
    Runs the ETL process for the log events.
    It proccess the result into parquets tables (time, playSongs, users)
    """
    event_schema = stt([
    r('artist', ST(), True),
    r('auth', ST(), True),
    r('firstName', ST(), True),
    r('gender', ST(), True),
    r('itemInSession', IT(), True),
    r('lastName', ST(), True),
    r('length', DT(), True),
    r('level', ST(), True),
    r('location', ST(), True),
    r('method', ST(), True),
    r('page', ST(), True),
    r('registration', DT(), True),
    r('sessionId', LT(), True),
    r('song', ST(), True),
    r('status', IT(), True),
    r('ts', LT(), True),
    r('userAgent', ST(), True),
    r('userId', ST(), True)])

    log_path = input_data + 'log-data/*'

    df = spark.read.json(log_path, schema = event_schema)
    
    df = df.filter(df["page"] == "NextSong")
      
    w = Window.partitionBy("userId").orderBy(desc("ts"))
    
    user_df = df.select(df["userId"], df["ts"], df["firstName"], \
                        df["lastName"], df["gender"], df["level"]) \
            .withColumn("maxTs",  sql_max("ts").over(w)) \
            .where(col("ts") == col("maxTs")) \
            .drop("maxTs") \
            .drop("ts") 

    users_table = user_df.filter(user_df["userId"].isNotNull()) \
                 .withColumnRenamed("userId", "user_id") \
                 .withColumnRenamed("firstName", "first_name") \
                 .withColumnRenamed("lastName", "last_name")
    

    users_table.write.parquet(output_data + "users")

    get_timestamp = udf(lambda epoch: datetime.fromtimestamp(epoch/1000).strftime("%Y%m-%d %H:%M:%S"))
    df = df.withColumn("timestamp",  get_timestamp("ts"))
    
    get_datetime = udf(lambda epoch: datetime.fromtimestamp(epoch/1000).strftime("%Y-%m-%d"))
    df = df.withColumn("datetime",  get_datetime("ts"))
    
    time_table = df.filter(df["ts"].isNotNull()) \
                  .select(df["timestamp"]) \
                  .withColumnRenamed("timestamp", "start_time") \
                  .withColumn("hour", hour("start_time")) \
                  .withColumn("day", dayofmonth("start_time")) \
                  .withColumn("week", weekofyear("start_time")) \
                  .withColumn("month", month("start_time")) \
                  .withColumn("year", year("start_time")) \
                  .withColumn("weekday", dayofweek("start_time"))
    
    time_table.write.partitionBy("year", "month").parquet(output_data + "time")

    artists_table = spark.read.parquet(output_data + "artists")
    
    song_df = spark.read.parquet(output_data + "songs")
    
    df.createOrReplaceTempView("staging_log")
    
    song_df.createOrReplaceTempView("staging_songs")
    artists_table.createOrReplaceTempView("staging_artists")
    
    spark.udf.register("uuid4", lambda : str(uuid4()), ST())
    
    songplays_table = spark.sql('''
        SELECT uuid4() as songplay_id, 
            l.datetime as start_time,
            l.userId as user_id,
            l.level as level,
            s.song_id,
            a.artist_id,
            l.sessionId as session_id,
            l.location,
            l.userAgent as user_agent
        FROM staging_log as l
        LEFT join staging_songs as s 
            on (l.song = s.title and  l.length = s.duration)
        LEFT join staging_artists as a 
            on (l.artist = a.name and a.artist_id = s.artist_id)
    ''')

    # I dont know how to partition by year and month
    songplays_table.write.partitionBy("start_time").parquet(output_data + "songPlay")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3-output-directory/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
