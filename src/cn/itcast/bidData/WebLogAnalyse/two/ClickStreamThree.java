package cn.itcast.bidData.WebLogAnalyse.two;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.util.BeanUtil;

import cn.itcast.bidData.WebLogAnalyse.one.WebLogBean;

/**
true
111.193.224.9
-
2013-09-18 07:17:25
/hadoop-family-roadmap/
200
11715
"https://www.google.com.hk/"
"Mozilla/5.0(Macintosh;IntelMacOSX10_8_5)AppleWebKit/537.36(KHTML,likeGecko)Chrome/29.0.1547.57Safari/537.36"
 */
/*
 * 
 * 将清洗之后的日志梳理出点击流pageviews模型数据
 * 输入数据是清洗过后的结果数据
 * 区分出每一次会话，给每一次visit（session）增加session-id（随机uuid）
 * 梳理出每一次会话中所访问的每个页面（请求时间，停留时长，以及该页面在这次session中的序号）
 * 保留referral_url,body_bytes_send,useragent
 * Session userid           时间                         访问页面URL    停留时长 第几步
 * S001    User01 2012-01-01 12:31:12    /a/....     30    1
 * 
 * */
public class ClickStreamThree {
	static class ClickStreamMapper extends Mapper<LongWritable, Text, Text, WebLogBean>{
		Text k = new Text();
		WebLogBean v = new WebLogBean();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fileds = line.split("\001");
			if(fileds.length<9) return;
			//将切分出来的各字段set到WebLogBean中
			v.set("true".equals(fileds[0]) ? true : false, fileds[1], fileds[2], 
					fileds[3], fileds[4], fileds[5], fileds[6],
					fileds[7], fileds[8]);
			//只有有效记录才进入后续处理
			if(v.isValid()){
				//同一ip地址下的数据先归纳出来
				k.set(v.getRemote_addr());
				context.write(k, v);
			}
		}
		
		static class ClickStreamReducer extends Reducer<Text, WebLogBean, NullWritable, Text>{
			Text v = new Text();

			@Override
			protected void reduce(Text key, Iterable<WebLogBean> values,Context context)
					throws IOException, InterruptedException {
				ArrayList<WebLogBean> beans = new ArrayList<WebLogBean>();
				//先将一个用户的所有访问记录中的时间拿出来排序
				try {
					try {
						for(WebLogBean bean : values){
							WebLogBean webLogBean = new WebLogBean();
							BeanUtils.copyProperties(webLogBean, bean);		
							beans.add(webLogBean);
						}					
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					//将bean按时间先后顺序排序
					Collections.sort(beans, new Comparator<WebLogBean>() {
						@Override
						public int compare(WebLogBean o1, WebLogBean o2) {
							try {
								Date d1 = toDate(o1.getTime_local());
								Date d2 = toDate(o2.getTime_local());
								if(d1 == null || d2 == null){
									return 0;
								}
								return d1.compareTo(d2);
							} catch (ParseException e) {
								e.printStackTrace();
							}
							return 0;
						}				
					});
					
					/*
					 * 以下逻辑为：从有序bean中分辨出各次visit,并对一次visit中所访问的page
					 * 按顺序标号step
					 * */
					
					int step =1;
					String session = UUID.randomUUID().toString();
					for(int i =0;i<beans.size();i++){
						WebLogBean bean = beans.get(i);
						//如果仅有1条数据，则直接输出
						if(1==beans.size()){
							//设置默认停留市场为60s
							v.set(session+"\00"+key.toString()+"\001"+bean.getRemote_user()
									+"\001"+bean.getTime_local()+"\001"+bean.getRequest()+"\001"
									+step+"\001"+(60)+"\001"+bean.getHttp_referer()+"\001"+
									bean.getHttp_user_agent()+"\001"+bean.getBody_bytes_sent()+
									"\001"+bean.getStatus());
							context.write(NullWritable.get(), v);
							session = UUID.randomUUID().toString();
							break;
						}
						//如果不止1条数据，则将第一条跳过不输出，遍历第二条时再输出
						if(i == 0){
							continue;
						}
						
						//求近2次时间差
						long timeDiff = timeDiff(bean.getTime_local(),beans.get(i-1).getTime_local());
						//本次-上次时间差<30分钟，则输出前一次的页面访问信息
						if(timeDiff < 30*60*1000){
							v.set(session+"\00"+key.toString()+"\001"+beans.get(i-1).getRemote_user()
									+"\001"+beans.get(i-1).getTime_local()+"\001"+beans.get(i-1).getRequest()+"\001"
									+step+"\001"+(timeDiff/1000)+"\001"+beans.get(i-1).getHttp_referer()+"\001"+
									beans.get(i-1).getHttp_user_agent()+"\001"+beans.get(i-1).getBody_bytes_sent()+
									"\001"+beans.get(i-1).getStatus());
							context.write(NullWritable.get(), v);
							step++;
						}else{
							//本次-上次时间差>30分钟，则输出前一次的页面访问信息
							v.set(session+"\00"+key.toString()+"\001"+beans.get(i-1).getRemote_user()
									+"\001"+beans.get(i-1).getTime_local()+"\001"+beans.get(i-1).getRequest()+"\001"
									+step+"\001"+(60)+"\001"+beans.get(i-1).getHttp_referer()+"\001"+
									beans.get(i-1).getHttp_user_agent()+"\001"+beans.get(i-1).getBody_bytes_sent()+
									"\001"+beans.get(i-1).getStatus());
							context.write(NullWritable.get(), v);
							//新的这一次将被从新记录
							step = 1;
							session = UUID.randomUUID().toString();
						}
						//如果是最后一条，则直接输出本条
						if(i==beans.size()-1){
							v.set(session+"\00"+key.toString()+"\001"+bean.getRemote_user()
									+"\001"+bean.getTime_local()+"\001"+bean.getRequest()+"\001"
									+step+"\001"+(60)+"\001"+bean.getHttp_referer()+"\001"+
									bean.getHttp_user_agent()+"\001"+bean.getBody_bytes_sent()+
									"\001"+bean.getStatus());
							context.write(NullWritable.get(), v);
						}
				}
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
			}		
			private Date toDate(String timeStr) throws ParseException {
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.US);
				return df.parse(timeStr);
			}
			
			private long timeDiff(String timeStr1,String timeStr2) throws ParseException{
				Date d1 = toDate(timeStr1);
				Date d2 = toDate(timeStr2);
				return d1.getTime()-d2.getTime();
			}
		}
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			
			job.setJarByClass(ClickStreamThree.class);
			job.setMapperClass(ClickStreamMapper.class);
			job.setReducerClass(ClickStreamReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(WebLogBean.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, "F:/hadoop_file/传智点击流日志/one_process");
			FileOutputFormat.setOutputPath(job, new Path("F:/hadoop_file/传智点击流日志/pageViews/"));
			
			//向yarn集群提交这个job
			boolean res = job.waitForCompletion(true);
			System.exit(res ? 0:1);
			
		}
	}
	
}
