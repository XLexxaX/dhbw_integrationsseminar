package com.dhbw.spark.spark_examples;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Hello world!
 *
 */
public class DataFrameSpark {

	public static void main(String[] args) throws Exception {
		Path file = Paths.get("./src/main/resources/Text.txt");

		SparkSession spark = SparkSession.builder().master("local").appName("JavaStructuredNetworkWordCount").getOrCreate();

		// Read all the csv files written atomically in a directory
		StructType userSchema = new StructType().add("text", DataTypes.StringType);
		Dataset<Row> df = spark.read().option("sep", ";").schema(userSchema).csv(file.toString());
		
	    df.printSchema();
	    //df.show();
	    
	    df.createOrReplaceTempView("DATAFRAME");
	    spark.sql("select * from DATAFRAME").show();
	}
}
