package com.atguigu.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	

	
	//nihao dev1
	
	// 在windows下无法直接提交job到YARN，可以在Linux下开启一个eclipse提交！
	// 使用hadoop jar jar包   主类名
	/*public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Path inputPath=new Path("hdfs://hadoop101:9000/wordcount");
		// 输出路径必须不存在
		Path outPath=new Path("file:///home/atguigu/wordcount");
		
		// 创建一个Job
		Job job = Job.getInstance();
		
		// 每个Job都有一个Configuraton对象
		
		// 设置当前Job所在的jar包
		job.setJarByClass(WordCountDriver.class);
		
		// 设置Job的名称，可以通过名称追踪Job的运行
		job.setJobName("wc");
		// 设置Job运行时自定义的一系列组件
		
	//	job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// 明确Map设置输入和输出的key-value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 设置处理的数据的输入路径
		FileInputFormat.setInputPaths(job, inputPath);
		// 处理后结果的输出路径
		FileOutputFormat.setOutputPath(job, outPath);
		
		//提交Job
		job.waitForCompletion(true);
		
	}*/
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		/*Path inputPath = new Path("file:///D:\\workspace\\word\\wordcount");
		Path outPath = new Path("file:///D:\\workspace\\word\\wordout");
		
		Job job = Job.getInstance();
		
		job.setJarByClass(WordCountDriver.class);
		
		job.setJobName("wc");
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileInputFormat.setInputPaths(job, outPath);
		
		job.waitForCompletion(true);*/
		
		
		
		Path inputPath=new Path("hdfs://hadoop101:9000/wordcount");
//		// 输出路径必须不存在
//		Path outPath=new Path("file:///home/atguigu/wordcount");
		
//		Path inputPath = new Path("file:///D:\\workspace\\word\\wordcount");
		Path outPath = new Path("file:///D:\\workspace\\word\\wordout");
		
		// 创建一个Job
		Job job = Job.getInstance();
		
		// 每个Job都有一个Configuraton对象
		
		// 设置当前Job所在的jar包
		job.setJarByClass(WordCountDriver.class);
		
		// 设置Job的名称，可以通过名称追踪Job的运行
		job.setJobName("wc");
		// 设置Job运行时自定义的一系列组件
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// 明确Map设置输入和输出的key-value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 设置处理的数据的输入路径
		FileInputFormat.setInputPaths(job, inputPath);
		// 处理后结果的输出路径
		FileOutputFormat.setOutputPath(job, outPath);
		
		//提交Job
		job.waitForCompletion(true);
		
		
		
	}

}
