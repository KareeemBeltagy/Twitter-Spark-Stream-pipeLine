# Spark Stream Pipeline Documentation

## Project Overview 

The Spark Stream pipeline project is a data pipeline that retrieves, processes, stores, and analyzes Twitter data related to specific topics. It consists of four separate scripts that work together to collect tweets, store them on HDFS, create tables and perform transformations and aggregations. A Bash script coordinates the four scripts, and a Cron Tab triggers another Bash script periodically to ensure the pipeline's smooth running.

## Project Explanation 

The pipeline consists of five separate scripts, each explained as follows:

### 1. Twitter-Listener.py

- A long-running Python code that uses Twitter API to retrieve tweets of specific topics.
- The code retrieves tweets related to two famous football players (Messi and Cristiano Ronaldo) that will be used later for simple analysis.
- It sends the retrieved fields of the tweet over a local port in the form of JSON for the next stage.

### 2. Twitter-stream.py

- A long-running PySpark code that uses structured streaming to receive the sent tweets over a local port.
- It parses this data into a data frame, then makes some transformations upon it.
- Then, the data is stored on HDFS as Parquet files partitioned by year, month, day, and hour.

### 3. Hive_tables.hql

- A HiveQL script responsible for creating the following tables:
  - `Tweet_base` (Hive external table that holds all the stored data)
  - `Tweet_dim` (Hive external table) that creates the first dimension table.
  - `Metrics_dim` (Hive external table) that creates the second dimension table.
- Then the script continues to insert and update all the tables with the newly arrived data.

### 4. Tweet_fact.py

- A Spark SQL application that reads the data from the dimension tables, makes the transformations and aggregations required to perform the required analysis (measuring the publicity of the tweet topic at a specific point of time).
- Then, it stores the final fact table as an external Hive table on HDFS.

The previous scripts are then coordinated using a bash script. A script is responsible for running the Twitter API and the Spark stream, and a Cron Tab is used to trigger another Bash script periodically, which checks if the Spark application is up and running, then continues to execute the rest of the pipeline.

## Instructions:

To use this pipeline, follow these instructions:

1. Add the following crontab command: `*/10 * * * * ./pipe2.bash >/dev/null 2>&1`
2. Run the script named `pipestart.bash`.
