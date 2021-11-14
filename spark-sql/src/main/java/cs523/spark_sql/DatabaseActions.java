package cs523.spark_sql;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

//import NBAConsumer.MyPair;
import cs523.spark_sql.TweetInfo;

public class DatabaseActions implements Serializable {

	private static final long serialVersionUID = 1L;
	private static DatabaseActions dbAction;
	transient static Configuration localConfig;

	private static final String TABLE_NAME = "tweet_info";
	private static final String COLUMN_FAMILY = "cf";

	public static DatabaseActions getDBAction() {
		if (dbAction == null) {
			dbAction = new DatabaseActions();
		}
		return dbAction;
	}

	// create table in HBase
	public void createTable() {
		try (
		Connection connection = ConnectorToHBase.getConnection();
				Admin admin = connection.getAdmin()){
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor("rowKey").setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(COLUMN_FAMILY).setCompressionType(Algorithm.NONE));

			System.out.print("Creating table.... ");

			if (!admin.tableExists(table.getTableName())) {
				System.out.println("Here is creating");
				System.out.println("Here is creating");
				System.out.println("Here is creating");
				System.out.println("Here is creating");
				System.out.println("Here is creating");
				admin.createTable(table);
			}
			System.out.println("Table is now created!");
		} catch (IOException e) {
			System.out.println(e.getMessage()+"------ERR");
	    }
	}

	// insert data into HBase
//	public void insertData(Configuration config, JavaRDD<TweetInfo> perStatRecord) throws IOException {
//		Job mrJob = Job.getInstance(config);
//		mrJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
//		mrJob.setOutputFormatClass(TableOutputFormat.class);
//		
//		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = perStatRecord.mapToPair(new MyPair());
//		hbasePuts.saveAsNewAPIHadoopDataset(mrJob.getConfiguration());
//	}

}
