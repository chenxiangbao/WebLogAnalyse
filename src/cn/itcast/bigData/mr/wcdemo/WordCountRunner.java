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
		
		//�������ǵ�ҵ���߼�mapper������key��value����������
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(IntWritable.class);
		
		//�������ǵ�ҵ���߼�Reduce������key��value����������
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(IntWritable.class);
		//ָ��Ҫ������ļ���������λ��
		//FileInputFormat.setInputPaths(wcjob,new Path(args[0]));
		//FileInputFormat.setInputPaths(wcjob, "C:/Users/11447/Desktop/hello.txt");
		FileInputFormat.setInputPaths(wcjob, "C:/Users/11447/Desktop/hello.txt");
		//ָ��������֮��Ľ�������λ��
		//FileOutputFormat.setOutputPath(wcjob,new Path(args[1]));
		FileOutputFormat.setOutputPath(wcjob, new Path("C:/Users/11447/Desktop/output/"));
		
		//��yarn��Ⱥ�ύ���job
		boolean res = wcjob.waitForCompletion(true);
		System.exit(res ? 0:1);
		
		
	}
}
