package com.pierre.spark.connection;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConnection {
	
	
	// A name for the spark instance. Can be any string
	private static String appName = "Pierre";
	// Pointer / URL to the Spark instance - embedded
	private static String sparkMaster = "local[2]";
	
	private static JavaSparkContext sparkContext = null;
	private static String tempDir = "file:///c:/temp/spark-warehouse";
	private static SparkSession sparkSession = null;


	public static void getSparkConnection(String environment) {

		if (sparkContext == null) {

			SparkConf configuration;
			
			System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");

			configuration = new SparkConf()
					.setAppName(appName)
					.setMaster(sparkMaster)
					.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

			if (environment == "server") {
				configuration = new SparkConf();
			}

			// Create Spark Context from configuration
			sparkContext = new JavaSparkContext(configuration);

			sparkSession = SparkSession.builder().appName(appName).master(sparkMaster)
					.config("spark.sql.warehouse.dir", tempDir).getOrCreate();

			if (environment == "server") {
				sparkSession = SparkSession.builder().getOrCreate();
			}

		}

	}

	public static JavaSparkContext getContext(String environment) {

		if (sparkContext == null) {
			getSparkConnection(environment);
		}
		return sparkContext;
	}

	public static SparkSession getSession(String environment) {
		if (sparkSession == null) {
			getSparkConnection(environment);
		}
		return sparkSession;
	}

}
