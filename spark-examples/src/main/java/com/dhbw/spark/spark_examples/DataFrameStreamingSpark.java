package com.dhbw.spark.spark_examples;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.SessionWindow;
import org.apache.spark.sql.catalyst.expressions.WindowSpec;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
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
public class DataFrameStreamingSpark {

	public static void main(String[] args) throws Exception {
		Path file = Paths.get("./src/main/resources/Text2.txt");

		SparkSession spark = SparkSession.builder().master("local").appName("DataFrameStreaming")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		// Read all the csv files written atomically in a directory
		StructType userSchema = new StructType().add("text", DataTypes.StringType);
		userSchema.add("timestamp", DataTypes.TimestampType);

		// Create DataFrame representing the stream of input lines from connection to
		// localhost:9999
		Dataset<Row> lines = spark.readStream().format("socket").option("host", "localhost").option("port", 9999)
				.load();

		Dataset<Row> timedLines = lines.withColumn("timestamp", functions.current_timestamp());
		
		timedLines.printSchema();
		
		
		//count everything in a 10 second window every 2 seconds;
		Dataset<Row> wordCounts = timedLines
				.groupBy(functions.window(functions.col("timestamp"), "10 seconds", "2 seconds"), timedLines.col("value")).count()
				.orderBy(functions.col("window").desc());//.limit(1);
		
		//Trigger=2seconds if ready, print to console.
		StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").option("truncate", "false")
				.trigger(Trigger.ProcessingTime(2000)).start();

		query.awaitTermination();

	}
}
