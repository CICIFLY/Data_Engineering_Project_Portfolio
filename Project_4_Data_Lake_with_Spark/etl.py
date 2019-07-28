# import the libraries 
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# set up the configration 
config = configparser.ConfigParser()

# read in credentials for S3 full access 
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config.get('CREDENTIALS', 'AWS_ACCESS_KEY_ID' )
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('CREDENTIALS', 'AWS_SECRET_ACCESS_KEY')

# initialize a spark session on aws hadoop
# return spark session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Read in song data from S3
    transform the data to create dimensional tables
    load the data back to S3 in parquet files  
    
    input: spark session, input datain json format and output data as s3 path    
    '''
    
    # get filepath to song data file
    # try with one file 
    #song_data = input_data + "song_data/A/B/C/TRABCEI128F424C983.json"
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    song_df = spark.read.json(song_data)
    print("Song data schema:")
    song_df.printSchema()
    print("Total records in song data is: ")
    print(song_df.count())

    # extract columns to create songs table
    songs_table = song_df.select(['song_id','song_title','artist_id','year','duration']).dropDuplicates().collect()
    
    # write songs table to parquet files partitioned by year and artist     
    songs_table.write.partitionBy('year','artist_id').parquet(output_data + "/songs_table.parquet")

    # extract columns to create artists table
    artists_table = song_df.select(['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude'])\
    .dropDuplicates().collect()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "/artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    
    '''
    Read in log data from S3
    transform the data to create dimensional tables
    load the data back to S3 in parquet files     
    '''
    
    # get filepath to log data file
    # try with one file
    #log_data = input_data + "log_data/2018/11/2018-11-12-events.json"
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for songplays
    log_df = log_df.filter(log_df.page=='NextSong')   

    # extract columns for users table    
    uers_table = log_df.select(['user_id', 'user_first_name', 'user_last_name', 'gender', 'level']).dropDuplicates().collect()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "/users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0)
                        .strftime('%Y-%m-%d %H:%M:%S'), TimestampType() )
    
    log_df = log_df.withColumn('ts', get_timestamp(log_df.ts))  
    log_df = log_df.withColumn('start_time', get_timestampe(log_df.ts))     
    log_df = log_df.withColumn('hour', get_timestamp(log_df.ts).hour)  
    log_df = log_df.withColumn('day' , get_timestamp(log_df.ts).day)
    log_df = log_df.withColumn('week' , get_timestamp(log_df.ts).week)
    log_df = log_df.withColumn('month' , get_timestamp(log_df.ts).month)
    log_df = log_df.withColumn('year' , get_timestamp(log_df.ts).year)
    log_df = log_df.withColumn('weekday' , get_timestamp(log_df.ts).weekday)
    log_df.printSchema()
    log_df.head(1)
    
    # extract columns to create time table
    time_table = log_df.select(['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']).dropDuplicates().collect()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data + "/time_table.parquet")

    # read in song data to use for songplays table
    #song_data = input_data + "song_data/A/B/C/TRABCEI128F424C983.json"
    song_data = input_data + "song_data/*/*/*/*.json"

    song_df = spark.read.json(song_data)  

    # extract columns from joined song and log datasets to create songplays table 
    # join two dataframes together by using the shared column 
    songplays_table = song_df.join(log_df, (song_df.artist_name == log_df.artist_name) &      
                                   (song_df.song_title == log_df.song_title) )
    songplays_table.withColumn("songplay_id",  monotonically_increasing_id())
    songplays_table = songplays_table.select( ['start_time', 'user_id', 'level', 'song_id', 
    'artist_id', 'sessionId', 'user_location','userAgent']).dropDuplicates().collect()
    songplays_table.printSchema()
    songplays_table.head(1)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + "/songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"      
    # if you want the output in a different bucket , then you need to create a new bucket first "s3a://project4_output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
