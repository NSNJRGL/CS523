package cs523.spark_sql;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class ConnectorToHBase {
	private static Connection myConnection;
	
	public static Connection getConnection (){
		if (myConnection == null){
			try {
				Configuration myConfig = HBaseConfiguration.create();
				myConnection = ConnectionFactory.createConnection(myConfig);
			}catch (IOException e) {
				System.out.println(e.getMessage().toString());
				System.exit(0);
			}
		}
		return myConnection;
	}

}
