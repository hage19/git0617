package com.atguigu.mr.findFriend;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// 输入： C(友)：A-E-G-H（用户列表）
//      D（友）： A-E-G-H
// 在进入map方法处理时，保证用户列表按照名字排序！
// 输出：A-E（用户-用户）：C（友） 
public class FindFriendMapper2 extends Mapper<Text, Text, Text, Text>{
	
	private Text keyOut=new Text();
	
	@Override
	protected void map(Text friend, Text users, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] userStrs = users.toString().split("-");
		//保证用户列表按照名字排序
		Arrays.sort(userStrs);
		
		for(int i=0;i<userStrs.length-1;i++) {
			
			for(int j=i+1;j<userStrs.length;j++) {
				
				keyOut.set(userStrs[i]+"-"+userStrs[j]);
				
				context.write(keyOut, friend);
				
			}
		}
		
	}

}
