package com.atguigu.mr.findFriend;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 输入：A-E（用户-用户）：C（友）  {A-E,C;A-E,G....}
// 输出：A-E:C,G
public class FindFriendReducer2 extends Reducer<Text, Text, Text, Text>{
	
	private Text valueOut=new Text();
	
	@Override
	protected void reduce(Text user, Iterable<Text> friends, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		StringBuffer sb = new StringBuffer();
		
		for (Text friend : friends) {
			
			sb.append(friend.toString()+",");
			
		}
		
		valueOut.set(sb.toString());
		
		context.write(user, valueOut);
		
		
	}

}
