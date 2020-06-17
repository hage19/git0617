package com.atguigu.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//import java.io.IOException;

/*import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
*/
/*
 * 1.X api : 包名以mapred结尾！ 
 * 使用2.x api ： 包名以mapreduce结尾！
 * 
 * 1.完成Map阶段处理数据的核心逻辑
 * 		① MapTask负责当前，一片数据的处理
 * 		② 先从这片数据中，将数据读进来
 * 		③ 在Mapper中对读入的记录进行处理
 * 
 * 自定义的Mapper必须继承Mapper!
 * 		
 * 
 * 2. InputFormat:  输入格式！ 代表程序读入的文件的控制类！
 * 			getSplits() : 负责对输入数据进行切片
 * 			createRecordReader(): 创建一个RecordReader，RecordReader读取记录到Mapper中！
 * 
 * 			默认系统使用的是TextInputFormat,会将普通的文本文件打散成行，再进行处理！
 * 			默认使用的是LineRecordReader！
 * 				默认LineRecordReader读入的数据key是当前行的偏移量(offset)；
 * 									value是当前行的内容！
 * 			
 * 3.  Mapper在处理数据的形式： 读入一组key-value,进行处理！将处理结果以key-value进行输出！
 * 	   KEYIN:  读入的key-value，key的类型！LongWritable！
 *     VALUEIN: 读入的key-value，value的类型！Text！
 *     				需要参考当前使用的InputFormat，创建的RecordReader里面指定的key-value类型！
 *     KEYOUT: 输出的key-value，key的类型！
 *     VALUEOUT:  输出的key-value，value的类型！
 *     				类型，可以自己定义！
 *     					期望输出： (单词，出现次数)
 *     
 *     输入和输出的key-value都涉及在网络的不同节点进行通信！ 类型，必须实现序列化接口！默认使用的是简化版的Hadoop提供的序列化接口
 *     			writeable!
 *     
 *   4. Mapper的工作流程
 *   			针对输入的key-value，产生中间介质的key-value!
 *   
 *   			① 首先调用setUp(),调用一次！ 类比初始化方法！
 *   			② 每一对输入的key-value，都会备map()调用处理！
 *   			③最终调用cleanup()!
 *   
 *   5. 重写map()，来实现自己的处理逻辑！
 * 		
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
/*	private Text keyOut = new Text();
	private IntWritable valutOut = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		for (String word : words) {
			context.write(keyOut, valutOut);
		}
		
		
	}
	*/
	
	
	private Text keyOut = new Text();
	private IntWritable valueOut = new IntWritable(1);
	/*
	 * key： 当前处理的输入的key
	 * value： 当前处理的输入的value：  hello hi how are you 
	 * context： 上下文对象，获取Job全局的设置信息！*/
	 
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String[] words = value.toString().split(" ");
		
		for (String word : words) {
			
			keyOut.set(word);
			// write只会写出对象中的值！
			context.write(keyOut, valueOut);
			
		}
		
	}
	

}
