package cn.itcast.bigData.mr.wcdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountRunner {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		
		conf.set("fs.defaultFS", "file:///");
		Job wcjob = Job.getInstance(conf);

		wcjob.setJarByClass(WordCountRunner.class);
		
		wcjob.setMapperClass(WordCountMapper.class);
		wcjob.setReducerClass(WordCountReduce.class);
		
		//设置我们的业务逻辑mapper类的输出key和value的数据类型
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(IntWritable.class);
		
		//设置我们的业务逻辑Reduce类的输出key和value的数据类型
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(IntWritable.class);
		//指定要处理的文件数据所在位置
		//FileInputFormat.setInputPaths(wcjob,new Path(args[0]));
		//FileInputFormat.setInputPaths(wcjob, "C:/Users/11447/Desktop/hello.txt");
		FileInputFormat.setInputPaths(wcjob, "C:/Users/11447/Desktop/hello.txt");
		//指定处理完之后的结果保存的位置
		//FileOutputFormat.setOutputPath(wcjob,new Path(args[1]));
		FileOutputFormat.setOutputPath(wcjob, new Path("C:/Users/11447/Desktop/output/"));
		
		//向yarn集群提交这个job
		boolean res = wcjob.waitForCompletion(true);
		System.exit(res ? 0:1);
		
		
	}
}
