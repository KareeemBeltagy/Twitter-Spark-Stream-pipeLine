# Import your dependecies
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
import time


# Starting pyspark session 
spark = SparkSession.builder.appName("TwitterStream").getOrCreate()

# read the tweet data from socket
tweet_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 7777) \
    .load()    


# Define the schema for the JSON object
schema = StructType([
    StructField("created_at", StringType(), True),
    StructField("tweet_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("retweet_count", IntegerType(), True),
    StructField("reply_count", IntegerType(), True),
    StructField("like_count", IntegerType(), True),
    StructField("impression_count", IntegerType(), True)
])

parsed_tweet_df = tweet_df.select(from_json(col("value"), schema).alias("data")).select("data.*")   
parsed_tweet_df = parsed_tweet_df.withColumn("created_at", date_format('created_at', 'yyyy-MM-dd HH:mm:ss'))\
                                  .withColumn("year", year("created_at"))\
                                  .withColumn("month", month("created_at"))\
                                  .withColumn("day", dayofmonth("created_at"))\
                                  .withColumn("hour", hour("created_at"))
# drop created_at column
parsed_tweet_df = parsed_tweet_df.drop("created_at")
#rearranging columns
parsed_tweet_df = parsed_tweet_df.select("year","month","day","hour","tweet_id","text","retweet_count","reply_count",\
                                         "like_count","impression_count")

# saving stream to parquet sink
writeTweet = parsed_tweet_df\
        .writeStream\
        .format("parquet")\
        .outputMode("append")\
        .option("path", "/twitter-landing-data")\
        .partitionBy("year","month","day","hour")\
        .trigger(processingTime="2 seconds")\
        .option("checkpointLocation", "/checkpointLocation")\
        .start()
                                         
writeTweet.awaitTermination()
                                     