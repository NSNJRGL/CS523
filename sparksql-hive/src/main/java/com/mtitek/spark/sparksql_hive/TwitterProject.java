package com.mtitek.spark.sparksql_hive;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TwitterProject {
	 public static void main(String[] args) throws AnalysisException {
	        final SparkConf sparkConf = new SparkConf();

	        sparkConf.setMaster("local");

	        sparkConf.set("hive.metastore.uris", "thrift://localhost:9083");

	        final SparkSession sparkSession = SparkSession.builder().appName("Spark SQL-Hive").config(sparkConf)
	                .enableHiveSupport().getOrCreate();

	    
	        sparkSession.sql("DROP TABLE IF EXISTS tableTweets8");

            sparkSession.sql("CREATE TABLE tableTweets8 ( name STRING, followers STRING)"+

                                 "ROW FORMAT DELIMITED FIELDS TERMINATED BY ':' ");

	        sparkSession.sql("insert into table tableTweets8 select name,followers from dataTable4 limit 10");

	        Dataset<Row> tabledata = sparkSession.sql("select * from tableTweets8 limit 10");
	        tabledata.show();
	    }

}
