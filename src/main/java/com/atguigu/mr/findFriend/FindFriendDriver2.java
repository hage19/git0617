package com.atguigu.mr.findFriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FindFriendDriver2 {
	
public static void main(String[] args) throws Exception {
		
		Path outPutPath = new Path("D:/workspace/word/outfrient1");
		
		Configuration conf = new Configuration();
		
		FileSystem fs=FileSystem.get(conf);
		
		if (fs.exists(outPutPath)) {
			
			fs.delete(outPutPath, true);
		}
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FindFriendDriver2.class);
		
		job.setJobName("flowbean");
		
		job.setMapperClass(FindFriendMapper2.class);
		
		job.setReducerClass(FindFriendReducer2.class);
		
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, outPutPath);
		
		FileInputFormat.setInputPaths(job, new Path("D:/workspace/word/outfrient"));
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		
		job.waitForCompletion(true);
		
	}

}
