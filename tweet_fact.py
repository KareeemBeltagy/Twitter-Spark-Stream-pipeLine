from os.path import abspath
import pyspark 
from pyspark.sql import SparkSession
# warehouse_location points to hive extrernal tables 
warehouse_location = abspath('/twitter-raw-data/')
# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .appName("SparkFactTable") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()
# Reading hive dimension tables to DF
df_tweetdim   =spark.read.table("tweet_dim")
df_metricsdim =spark.read.table("metrics_dim")

# Removing any duplicates and discarding some of the column in metrics data farme (usually the values = 0) 
df_tweetdim = df_tweetdim.select( "year", "month", "day", "hour", "tweet_id", "text").dropDuplicates()
df_metricsdim = df_metricsdim.select("year", "month", "day", "hour", "tweet_id", "retweet_count", "impression_count").dropDuplicates()

# Extracting the names of the players as as indicator of the topic of the tweet 
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import re

# Define UDF to extract names from text
def extract_names(text):
    text = text.lower()
    pattern = r"(?i)\b(messi|cristiano)\b'?s?"
    matches = re.findall(pattern, text)
    if matches:
        return ", ".join(set(matches))
    else:
        return None
# Apply the UDF to a DataFrame column (text) and create column topic
extract_names_udf = F.udf(extract_names, StringType())
df_tweetdim = df_tweetdim.withColumn("Tweet_topic", extract_names_udf("text"))    

# getting only the required columns for calculations in  df_tweetdim data frame 
df_tweetdim = df_tweetdim.select( "year", "month", "day", "hour", "tweet_id","Tweet_topic")

# joining both dataframes 
df_tweet = df_tweetdim.join(df_metricsdim , ["year","month","day","hour","tweet_id"],'inner')\
                        .select(df_tweetdim["year"], df_tweetdim["month"], df_tweetdim["day"], df_tweetdim["hour"], "tweet_id","Tweet_topic","retweet_count", "impression_count")
# creating temp view 
df_tweet.createOrReplaceTempView("vw_tweet_fact")

# calculating the average of retweet_count,impression_count as an indicator of how popular is this topic 
# on specific point of time. 
df_fact = spark.sql("select year,month,day,hour,Tweet_topic,count(Tweet_topic) as Count_tweets,\
            round(avg((retweet_count+impression_count)/2),1) as metric\
          from vw_tweet_fact \
           group by year,month,day,hour,Tweet_topic order by year,month,day,hour,Tweet_topic" )
# Create Hive External table 
df_fact.write.mode('append')\
        .option("path", "/twitter-processed-data")\
        .saveAsTable("tweet_fact_processed")
# stop spark session after excution 
spark.stop()           