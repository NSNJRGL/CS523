package cs523.spark_sql;

public class App {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("-----");
		
		// hbase
		DatabaseActions da = new DatabaseActions();
		da.createTable();
		
		// spark
		SparkSql ss = new SparkSql();
		ss.runSS();
	}

}
