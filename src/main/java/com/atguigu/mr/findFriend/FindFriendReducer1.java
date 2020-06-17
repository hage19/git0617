package com.atguigu.mr.findFriend;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 输入： {C-A，C-E，C-G，C-H}
// 输出： C(友)：A-E-G-H（用户列表）
public class FindFriendReducer1 extends Reducer<Text, Text, Text, Text>{
	
	private Text valueOut=new Text();
	
	@Override
	protected void reduce(Text friend, Iterable<Text> users, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		StringBuffer sb = new StringBuffer();
		
		for (Text user : users) {
			
			sb.append(user.toString()+"-");
			
		}
		
		valueOut.set(sb.toString());
		
		context.write(friend, valueOut);
		
		
	}

}
