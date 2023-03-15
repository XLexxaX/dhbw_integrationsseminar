package com.dhbw.spark.spark_examples;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class DeltaLakeSpark {

	public static void main(String[] args) throws Exception {
		Path file = Paths.get("./src/main/resources/Text.txt");
		Path file2 = Paths.get("./src/main/resources/iceberg/");
		Path file3 = Paths.get("./src/main/resources/iceberg_snapshots/");

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		conf.set("spark.jars.packages", "io.delta:delta-core_2.13:2.2.0");
		conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
	    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
		
		SparkSession spark = SparkSession.builder().master("local").appName("JavaStructuredNetworkWordCount").config(conf).getOrCreate();

		/*
		StructType userSchema = new StructType().add("text", DataTypes.StringType);
		Dataset<Row> df = spark.read().option("sep", ";").schema(userSchema).csv(file.toString());
		
	    
	    df.createOrReplaceTempView("DATAFRAME");
	    spark.sql("select * from DATAFRAME").show();

	    List<Row> nums = new ArrayList<Row>();
	    nums.add(RowFactory.create("value1"));
	    Dataset<Row> df2 = spark.createDataFrame(nums, userSchema);
	    
	    spark.sql("CREATE or REPLACE TABLE DATAFRAME_ICEBERG USING iceberg AS SELECT * FROM DATAFRAME");*/
		spark.sql("CREATE DATABASE delta");
		spark.sql("CREATE OR REPLACE TABLE delta.table (text string) USING delta");
		spark.sql("INSERT INTO delta.table VALUES ('hello')");
	}
}
