---------Creating base table ---------------------------------------------------------
create external table if not exists tweet_base (tweet_id string ,text string,
		retweet_count int,reply_count int,like_count int,impression_count int)
		PARTITIONED by (year int,month int,day int,hour int) 
		stored as parquet location '/twitter-landing-data';

MSCK REPAIR TABLE tweet_base;
desc tweet_base;
-------------creating dimension tables ---------------------------------------------------
create external table if not exists tweet_dim (tweet_id string ,text string)
		PARTITIONED by (year int,month int,day int,hour int) 
		stored as parquet location '/twitter-raw-data/tweet-dim-raw';
				
MSCK REPAIR TABLE tweet_dim;
desc tweet_dim;
--------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS metrics_dim (tweet_id string,retweet_count int,reply_count int,
		like_count int,impression_count int)
		PARTITIONED by (year int,month int,day int,hour int)
		stored as parquet location '/twitter-raw-data/metrics-dim-raw';
		
MSCK REPAIR TABLE metrics_dim;
desc metrics_dim;
-----------------------inserting data to dimesnion tables ----------------------------
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE tweet_dim PARTITION (year, month, day, hour)
SELECT 
    tweet_base.tweet_id,
    tweet_base.text,
    tweet_base.year,
    tweet_base.month,
    tweet_base.day,
    tweet_base.hour
FROM tweet_base
LEFT OUTER JOIN tweet_dim
ON tweet_base.tweet_id = tweet_dim.tweet_id
    AND tweet_base.year = tweet_dim.year
    AND tweet_base.month = tweet_dim.month
    AND tweet_base.day = tweet_dim.day
    AND tweet_base.hour = tweet_dim.hour
WHERE tweet_dim.tweet_id IS NULL;

INSERT INTO TABLE metrics_dim PARTITION (year, month, day, hour)
SELECT
	tweet_base.tweet_id,
	tweet_base.retweet_count,
	tweet_base.reply_count,
	tweet_base.like_count,
	tweet_base.impression_count ,
	tweet_base.year,
    tweet_base.month,
    tweet_base.day,
    tweet_base.hour
FROM tweet_base
LEFT OUTER JOIN metrics_dim
ON tweet_base.tweet_id =   metrics_dim.tweet_id
	AND tweet_base.year =  metrics_dim.year
    AND tweet_base.month = metrics_dim.month
    AND tweet_base.day =   metrics_dim.day
    AND tweet_base.hour =  metrics_dim.hour
WHERE metrics_dim.tweet_id IS NULL; 

