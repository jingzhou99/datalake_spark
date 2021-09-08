
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, dayofweek,hour, weekofyear, date_format
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType,LongType


# create spark session
def create_spark_session():
    """create a spark session """

    spark = SparkSession \
        .builder \
        .config("spark.debug.maxToStringFields", "100") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_path, output_path):
    """load song from s3, process and write back to `songs` and `artists` table 
    
    Args:
        spark(sparkSession): sparkSession created in function create_spark_session()
        input_path(str): path of source files 
        output_path(str): path of output dimensional tables 
    
    Return:
        None
    """
    
    # specify song file path
    song_data = input_path + 'song_data/A/A/*/*.json'
   
    # custom schema , then load json
    songschema = StructType([
      StructField("num_songs",IntegerType(),True),
      StructField("artist_id",StringType(),True),
      StructField("artist_latitude",DoubleType(),True),
      StructField("artist_longitude",DoubleType(),True),
      StructField("artist_location",StringType(),True),
      StructField("artist_name",StringType(),True),
      StructField("song_id",StringType(),True),
      StructField("title",StringType(),True),
      StructField("duration",DoubleType(),True),
      StructField("year",IntegerType(),True)
      ])
    
    song_df = spark.read.schema(songschema).json(song_data)
    
    
    # extract columns to create songs table
    songs_table = song_df.select(["song_id","title","artist_id","year","duration"]).dropDuplicates()
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_output_patch = output_path + 'songs.parquet'
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(songs_table_output_patch) 

    
    # extract columns to create artists table
    artists_table = song_df.select(["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table_output_patch = output_path +'artists.parquet'
    artists_table.write.mode("overwrite").parquet(artists_table_output_patch)


    
def process_log_data(spark, input_path, output_path):
    """load song from s3, process and write back to `users`,`time` and `songplays` table 
    
    Args:
        spark(sparkSession): sparkSession created in function create_spark_session()
        input_path(str): path of source files 
        output_path(str): path of output dimensional tables 
    
    Return:
        None
    """
    
    # specify logs file path
    logs_data = input_path + 'log_data/2018/*/*.json'

    # read log data file
    log_df = spark.read.json(logs_data)
    
    # inferred schema is not good enough, change following column data type manually
    trans1 = log_df.withColumn("userId", log_df["userId"].cast(IntegerType()))
    trans2 = trans1.withColumn("registration", log_df["registration"].cast(LongType()))
    trans3 = trans2.withColumn("itemInSession", log_df["itemInSession"].cast(IntegerType()))
    trans4 = trans3.withColumn("sessionId", log_df["sessionId"].cast(IntegerType()))
    log_newschema_df = trans4.withColumn("status", log_df["status"].cast(IntegerType()))


    # extract columns for users table, and dropDuplicates    
    user_table = log_newschema_df.select(["userId","firstName","lastName","gender","level"])
    unique_user_table = user_table.dropDuplicates()
    
    # write users table to parquet files
    users_table_output_patch = output_path + 'users.parquet'
    unique_user_table.write.mode("overwrite").parquet(users_table_output_patch)

    
    # get time dataframe  from original timestamp column
    t1 = log_newschema_df.select(["ts"])
        
    # use from_unixtime func convert epoch unix timestamp
    t2 = t1.withColumn("start_time", col("ts")).withColumn("datetime", from_unixtime(log_df["ts"]/1000))
    
    # use series of spark func to get all the columns
    t3 = t2 \
    .withColumn("hour", hour(col("datetime"))) \
    .withColumn("day", dayofmonth(col("datetime"))) \
    .withColumn("week", weekofyear(col("datetime"))) \
    .withColumn("month", month(col("datetime"))) \
    .withColumn("year", year(col("datetime"))) \
    .withColumn("dow", dayofweek(col("datetime")))\
    
    time_table = t3.select(["start_time","hour","day","week","month","year","dow"]).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table_output_patch = output_path + 'time.parquet'
    time_table.write.partitionBy("year","month").mode("overwrite").parquet(time_table_output_patch)

    
    
    # to get songplays fact table, need to join logs with other two table : song_table and artist_table, here use TempView and SparkSQL
    songs_table_output_patch = output_path + 'songs.parquet'
    song_parquet_df = spark.read.parquet(songs_table_output_patch)
    song_parquet_df.createOrReplaceTempView("song_parquet_view")
    
    artists_table_output_patch = output_path +'artists.parquet'
    artist_parquet_df = spark.read.parquet(artists_table_output_patch)
    artist_parquet_df.createOrReplaceTempView("artist_parquet_view")
    
    log_newschema_df.createOrReplaceTempView("logview")
    
    # extract columns to create songs table
    songplays = spark.sql("""
        SELECT  A.ts as start_time,
                A.userId,
                A.level,
                B.song_id,
                B.artist_id,
                A.sessionId,
                A.location,
                A.userAgent
        FROM    logview A
                    INNER JOIN
                song_parquet_view B ON A.song=B.title AND A.length=B.duration
                    INNER JOIN
                artist_parquet_view C ON A.artist=C.artist_name
        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table_output_patch = output_path + 'songplays.parquet'
    songplays.write.mode("overwrite").parquet(songplays_table_output_patch) 

    
    
def main():
    """Spark to create Data Warehouse, process json file and output parquet """

    # create spark session
    spark = create_spark_session()
    
    # specify source data file path 
    input_path = "s3a://udacity-dend/"
    
    # path for output Data Lake table files 
    output_path = "s3a://jingdatalake/"
    
    # call function to process songs and logs
    process_song_data(spark, input_path, output_path)    
    process_log_data(spark, input_path, output_path)
    

if __name__ == "__main__":
    main()
