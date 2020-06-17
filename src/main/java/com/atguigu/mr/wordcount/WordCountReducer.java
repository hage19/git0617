package com.atguigu.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * 1. 当前类必须继承Reducer类！
 * 
 * 2. KEYIN-VALUEIN: Mapper处理的输出的key-value类型！
 * 				key-(value,value,value)
 * 		KEYOUT-VALUEOUT: 自己通过reducer处理后的数据类型！
 * 				期望： (单词，次数)
 * 
 * 3. MapTask处理的数据，在进入Reducer方法之前，会经过排序和分组！
 * 			Map输出的记录： key-value,key-value
 * 				hadoop-1,hadoop-1,hive-1,apache-1
 * 			在进入reducer之前，这些记录，会被排序和分组！
 * 				排序： 默认根据key的字典顺序排列
 * 				分组： 相同的key的key-value会分到一个组
 * 				apache-{1}
 *              hadoop-{1,1}
 *              hive-{1}
 *              
 * 4. 没一组记录，都会被reduce()处理！
 * 
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	private IntWritable valueOut=new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		int sum=0;
		
		for (IntWritable value : values) {
			
			sum+=value.get();
		}
		
		valueOut.set(sum);
		
		context.write(key, valueOut);
		
		
	}
	
	
	
/*	private IntWritable valueOut = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		valueOut.set(sum);
		context.write(key, valueOut);
	}
*/
	
	
	
	 /*private IntWritable valueOut=new IntWritable();
	
	 @Override protected void reduce(Text key, Iterable<IntWritable> values,
	 Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws
	 IOException, InterruptedException {
	 
	 int sum=0;
	  
	 for (IntWritable value : values) {
	 
	 sum+=value.get(); 
	 }
	  
	 valueOut.set(sum);
	 
	 context.write(key, valueOut);
	 
	 }*/
}
