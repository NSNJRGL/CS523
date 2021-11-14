package cs523.spark_sql;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * Hello world!
 *
 */
public class SparkSql {
	
    public void runSparkSql() {
    	/*
    	 String driverName = "org.apache.hive.jdbc.HiveDriver";
         Class.forName(driverName);

         Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/userdb", "cloudera", "cloudera");
         Statement stmt = con.createStatement();
         stmt.executeQuery("CREATE EXTERNAL TABLE IF NOT EXISTS tweetdata(value STRING) LOCATION '/user/cloudera/rawtweets'");
         stmt.executeQuery("LOAD DATA LOCAL INPATH '/user/cloudera/tweets-warehouse/' OVERWRITE INTO TABLE tweetdata"); 
         
         stmt.executeQuery("CREATE EXTERNAL TABLE IF NOT EXIST cleantweets("+
         "t_created_at STRING,"+
         "t_text STRING,"+
         "t_location STRING,"+
         "t_followers_count INT,"+
         "t_statuses_count INT,"+
         "t_filter_level STRING,"+
         "t_lang STRING)"+
         "LOCATION '/user/cloudera/cleantweets'");
         
         stmt.executeQuery("INSERT INTO TABLE cleantweets SELECT "+
         "GET_JSON_OBJECT(tweetdata.value,'$.created_at'),"+
     	"GET_JSON_OBJECT(tweetdata.value,'$.text'),"+
     	"GET_JSON_OBJECT(tweetdata.value,'$.user.location'),"+
     	"GET_JSON_OBJECT(tweetdata.value,'$.user.followers_count'),"+
     	"GET_JSON_OBJECT(tweetdata.value,'$.user.statuses_count'),"+
     	"GET_JSON_OBJECT(tweetdata.value,'$.filter_level'),"+
     	"GET_JSON_OBJECT(tweetdata.value,'$.lang')"+
         "FROM tweetdata");
         
         con.close();*/
    	
		final SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local");
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        sparkConf.set("spark.sql.warehouse.dir", warehouseLocation);
        //sparkConf.set("spark.sql.warehouse.dir","hdfs://localhost/user/cloudera/tweets-warehouse");
        //sparkConf.set("hive.metastore.uris", "thrift://localhost:9083");
        
        
        
        
        final SparkSession spark = SparkSession.builder().appName("sparksql").config(sparkConf)
                .enableHiveSupport().getOrCreate();
        
//        Dataset<Row> rawtweets = spark.read().json("/home/cloudera/workspace/spark-streaming/output/");
        // TO DO FIX AFTER SPARK STREAMING
        Dataset<Row> rawtweets = spark.read().json("./data/");
        
        System.out.println("Schema\n=======================");
        rawtweets.printSchema();
        rawtweets.createOrReplaceTempView("rawtweets");
        
        Dataset<Row> cleantweets = spark.sql("SELECT user.location, user.statuses_count FROM rawtweets WHERE user.location != 'null'");
        System.out.println("\n\n clean tweets SQL Result\n=======================");
        cleantweets.show();
        cleantweets.createOrReplaceTempView("cleantweets");
        Dataset<Row> avgtweet = spark.sql("SELECT cleantweets.location, AVG(cleantweets.statuses_count) AS average FROM cleantweets GROUP BY cleantweets.location ORDER BY average DESC");
        System.out.println("\n\n top number of statuses per location Result\n=======================");
        avgtweet.show();
        
		
    }
}
