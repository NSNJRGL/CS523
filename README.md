# Twitter Streaming Data with <img src="https://spark.apache.org/images/spark-logo-trademark.png" width="150" />

## Overview

Twitter Streaming with Apache Spark, Apache Kafka, Hive, Hbase, SparkQL, and Tableau.

## How to run project

1. Getting Twitter API keys

    * Create a twitter account if you do not already have one. 
    * Go to https://apps.twitter.com/ and log in with your twitter credentials. 
    * Click "Create New App" 
    * Fill out the form, agree to the terms, and click "Create your Twitter application" 
    * In the next page, click on "API keys" tab, and copy your "API key" and "API secret". 
    * Scroll down and click "Create my access token", and copy your "Access token" and "Access token secret".

2. Open Terminal and start Kafka server: \
   `cd /opt/kafka_2.13-2.6.2/` \
   `bin/kafka-server-start.sh config/server.properties`
3. Execute TweetProducer.java of twitter-kafka project.

4. Execute JavaSparkApp.java of spark-streaming project.

5. Start `hive` and create new table:
   ```sql
   CREATE EXTERNAL TABLE tweet_data_table name STRING, country STRING, followers STRING)
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
   WITH SERDEPROPERTIES ('separatorChar' = ':', 'quoteChar' = '\')
   LOCATION '/user/cloudera/Tweets'
   TBLPROPERTIES ('hive.input.dir.recursive'='ture', 'hive.supports.subdirectories'='true',
   'hive.supports.subdirectories'='true', 'mapreduce.input.fileinputformat.input.dir.recursive'='true');
   ```
   
6. Create internal table
   ```sql
   CREATE TABLE report (name STRING, followers STRING)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY ":";
   ```
7. Load data into internal table
   ```sql
   LOAD DATA LOCAL INPATH '/home/cloudera/Tweet' OVERWRITE INTO TABLE report;
   ```

## Troubleshot

1. Sometime Hbase service dead and you must be restart by commands:
   ```
   sudo service hbase-master restart;
   sudo service hbase-regionserver restart;
   ```

## Reference

1. Cloudera [https://www.cloudera.com/](https://www.cloudera.com/)
2. Apache Spark-Streaming: [https://spark.apache.org/streaming/](https://spark.apache.org/streaming/)
3. Apache SQL: [http://spark.apache.org/sql/](http://spark.apache.org/sql/)
4. Apache Kafka: [https://kafka.apache.org/](https://kafka.apache.org/)
5. Apache Hive [https://hive.apache.org/](https://hive.apache.org/)
6. Apache HBase [https://hbase.apache.org/](https://hbase.apache.org/)
7. Tableau [https://www.tableau.com/](https://www.tableau.com/)
