package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDB {

	public void select() {
		SparkSession spark = SparkSession.builder()
				.appName("My application name")
				.config("option name", "option value")
				.master("dse://1.1.1.1?connection.host=1.1.2.2,1.1.3.3")
				.getOrCreate();

		Dataset<Row> employees = spark.sql("SELECT * FROM HIVE_TWEET");

		employees.show();
	}
}
