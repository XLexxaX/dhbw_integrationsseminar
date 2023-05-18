package com.dhbw.spark.spark_examples;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class WordCountSpark 
{
	public static void main(String[] args) throws Exception {
	    Path file = Paths.get("./src/main/resources/Text.txt");
	
	    SparkConf sparkConf = new SparkConf().setAppName("WordCountSpark").set("spark.master", "local");
	    sparkConf.set("spark.testing.memory", "2453012480");   
	
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    
	    JavaRDD<String> lines = ctx.textFile(file.toString(), 1);

	    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
	    JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
	    JavaPairRDD<String, Integer> counts 
	      = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

	    List<Tuple2<String, Integer>> output = counts.collect();
	    for (Tuple2<?, ?> tuple : output) {
	        System.out.println(tuple._1() + ": " + tuple._2());
	    } 
	    ctx.stop();
	    ctx.close();
	} 
} 
