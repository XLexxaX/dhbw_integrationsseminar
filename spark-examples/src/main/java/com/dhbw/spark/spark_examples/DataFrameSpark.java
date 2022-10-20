package com.dhbw.spark.spark_examples;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class DataFrameSpark 
{
	
	public static void main(String[] args) throws Exception {
	    Path file = Paths.get("./src/main/resources/Text.txt");
	
	    SparkConf sparkConf = new SparkConf().setAppName("DataFrameSpark").set("spark.master", "local");
	    
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    
	    @SuppressWarnings("deprecation")
		SQLContext sqlctx = new SQLContext(ctx);
	    
	    StructType schema = new StructType(new StructField[]{
	    	   new StructField("name", DataTypes.StringType, false, Metadata.empty())
	     } );
	    
	    
	    Dataset<Row> ds = sqlctx.read().format("csv")
	    		  .option("sep", ";")
	    		  .option("inferSchema", "true")
	    		  .option("header", "false")
	    		  .load(file.toString());
	    
	    Dataset<Row> df = sqlctx.createDataFrame(ds.javaRDD(), schema);
	    
	    System.out.println(df.collect());
	    df.printSchema();
	    df.show();
	    
	    ctx.stop();
	    ctx.close();
	}
}
