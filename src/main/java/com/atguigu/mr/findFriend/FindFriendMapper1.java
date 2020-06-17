package com.atguigu.mr.findFriend;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//A（key）:B,C,D,F,E,O （value）
// 输出： C-A 友-用户
public class FindFriendMapper1 extends Mapper<Text, Text, Text, Text>{
	
	private Text keyOut=new Text();
	
	@Override
	protected void map(Text valueOut, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		
		
		String[] friends = value.toString().split(",");
		
		for (String friend : friends) {
			
			keyOut.set(friend);
			
			context.write(keyOut, valueOut);
			
		}
		
	}

}
