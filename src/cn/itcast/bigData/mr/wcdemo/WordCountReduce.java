package cn.itcast.bigData.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context content)
			throws IOException, InterruptedException {
		int num = 0;
		for(IntWritable val:values){
			num+=val.get();
		}
		//System.out.println(key);
		content.write(key, new IntWritable(num));
	} 
}
