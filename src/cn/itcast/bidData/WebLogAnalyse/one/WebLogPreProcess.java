package cn.itcast.bidData.WebLogAnalyse.one;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * 对原始日志进行过滤，把符合要求的日志刷选出来
 * */
public class WebLogPreProcess {
	static class WebLogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		//用来存储网站url分类数据
		Set<String> pages = new HashSet<String>();
		Text k = new Text();
		NullWritable v = NullWritable.get();
		/*
		 * 从外部加载url分类数据
		 * */
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			pages.add("/about");
			pages.add("/black-ip-list/");
			pages.add("/cassandra-clustor/");
			pages.add("/finance-rhive-repurchase/");
			pages.add("/hadoop-family-roadmap/");
			pages.add("/hadoop-hive-intro/");
			pages.add("/hadoop-zookeeper-intro/");
			pages.add("/hadoop-mahout-roadmap/");
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			WebLogBean webLogBean = WebLogParser.parser(line);
			//过滤js/图片/css等静态资源
			WebLogParser.filtStaticResource(webLogBean, pages);
			/*if(!webLogBean.isValid())  return;*/
			k.set(webLogBean.toString());
			context.write(k, v);
		}
		/*
		 * true
		 * 111.193.224.9
		 * 2013-09-18 07:17:25
		 * /hadoop-family-roadmap/
		 * 200
		 * 11715
		 * "https://www.google.com.hk/"
		 * "Mozilla/5.0(Macintosh;IntelMacOSX10_8_5)AppleWebKit/537.36(KHTML,likeGecko)Chrome/29.0.1547.57Safari/537.36"
		 * */
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			conf.set("mapreduce.framework.name", "local");
			
			conf.set("fs.defaultFS", "file:///");	
			Job job = Job.getInstance(conf);
			
			job.setJarByClass(WebLogPreProcess.class);
			job.setMapperClass(WebLogPreProcessMapper.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			FileInputFormat.setInputPaths(job, "F:/hadoop_file/传智点击流日志/access.log.fensi");
			FileOutputFormat.setOutputPath(job, new Path("F:/hadoop_file/传智点击流日志/output/"));
			
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
		}
	}
}
