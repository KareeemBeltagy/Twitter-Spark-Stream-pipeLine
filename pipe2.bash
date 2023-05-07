# Get the list of running Spark applications
spark_apps=$(yarn application -list | awk '{if($3=="SPARK") print}')

# Check if spark stream is running
if echo "$spark_apps" | grep -q "twitter-stream.py"; then
   
    echo "************************stream is running************************"
	echo "************************Running dimms table script*******************"
	hive -f hive_tables.hql
	sleep 10
	echo "************************Running fact table script************************"
	/opt/spark3/bin/spark-submit --deploy-mode cluster ./tweet_fact.py 
else
    echo "Spark application is not running"
fi
