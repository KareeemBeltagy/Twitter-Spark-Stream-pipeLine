echo "************************************starting twitter_listener************************************"
/opt/spark3/bin/spark-submit ./twitter_listener.py &
sleep 5
echo "***********************************starting Spark stream***********************************"
/opt/spark3/bin/spark-submit --deploy-mode cluster ./twitter-stream.py &

