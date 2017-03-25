package com.pierre.spark.connection;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MySQLReadWrite {
	
	public static Dataset<Row> ReadFromMySQL(String MySQL_Table, String MySQL_IP, String MySQL_user,
			String MySQL_password, SparkSession sparkSession, String environment) {

		Properties connectionProperties = new Properties();
		String mySQL_connection_URL;
		Dataset<Row> inputdata;

		connectionProperties.setProperty("user", MySQL_user);
		connectionProperties.setProperty("password", MySQL_password);
		mySQL_connection_URL = "jdbc:mysql://" + MySQL_IP + "?user=" + MySQL_user + "&password=" + MySQL_password;

		if (environment == "server") {
			connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver");
		}
		inputdata = sparkSession.read().jdbc(mySQL_connection_URL, MySQL_Table, connectionProperties);
		return inputdata;
	}
	
	
	
	public static void WriteToMySQL(Dataset<Row> datasetExport, String MySQL_Table, String MySQL_IP, String MySQL_user,
			String MySQL_password, String environment) {

		Properties connectionProperties = new Properties();
		String mySQL_connection_URL;

		connectionProperties.setProperty("user", MySQL_user);
		connectionProperties.setProperty("password", MySQL_password);
		mySQL_connection_URL = "jdbc:mysql://" + MySQL_IP + "?user=" + MySQL_user 
				+ "&password=" + MySQL_password + "&rewriteBatchedStatements=true";

		if (environment == "server") {
			connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver");
		}
		
		datasetExport.write().jdbc(mySQL_connection_URL, MySQL_Table, connectionProperties);
	}

}
