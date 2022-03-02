package com.dhbw.mr.mr_examples;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendCountMR {

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			//TODO: Implement friend count mapper.

		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			//TODO: Implement friend count reducer.
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String in = "./src/main/resources/Friends.txt";
		String out = "./src/main/resources/friend_results";
		Job job = new Job(conf, "friend count");
		job.setJarByClass(FriendCountMR.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
