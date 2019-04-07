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
 * ����ϴ֮�����־����������pageviewsģ������
 * ������������ϴ����Ľ������
 * ���ֳ�ÿһ�λỰ����ÿһ��visit��session������session-id�����uuid��
 * �����ÿһ�λỰ�������ʵ�ÿ��ҳ�棨����ʱ�䣬ͣ��ʱ�����Լ���ҳ�������session�е���ţ�
 * ����referral_url,body_bytes_send,useragent
 * Session userid           ʱ��                         ����ҳ��URL    ͣ��ʱ�� �ڼ���
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
			//���зֳ����ĸ��ֶ�set��WebLogBean��
			v.set("true".equals(fileds[0]) ? true : false, fileds[1], fileds[2], 
					fileds[3], fileds[4], fileds[5], fileds[6],
					fileds[7], fileds[8]);
			//ֻ����Ч��¼�Ž����������
			if(v.isValid()){
				//ͬһip��ַ�µ������ȹ��ɳ���
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
				//�Ƚ�һ���û������з��ʼ�¼�е�ʱ���ó�������
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
					
					//��bean��ʱ���Ⱥ�˳������
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
					 * �����߼�Ϊ��������bean�зֱ������visit,����һ��visit�������ʵ�page
					 * ��˳����step
					 * */
					
					int step =1;
					String session = UUID.randomUUID().toString();
					for(int i =0;i<beans.size();i++){
						WebLogBean bean = beans.get(i);
						//�������1�����ݣ���ֱ�����
						if(1==beans.size()){
							//����Ĭ��ͣ���г�Ϊ60s
							v.set(session+"\00"+key.toString()+"\001"+bean.getRemote_user()
									+"\001"+bean.getTime_local()+"\001"+bean.getRequest()+"\001"
									+step+"\001"+(60)+"\001"+bean.getHttp_referer()+"\001"+
									bean.getHttp_user_agent()+"\001"+bean.getBody_bytes_sent()+
									"\001"+bean.getStatus());
							context.write(NullWritable.get(), v);
							session = UUID.randomUUID().toString();
							break;
						}
						//�����ֹ1�����ݣ��򽫵�һ������������������ڶ���ʱ�����
						if(i == 0){
							continue;
						}
						
						//���2��ʱ���
						long timeDiff = timeDiff(bean.getTime_local(),beans.get(i-1).getTime_local());
						//����-�ϴ�ʱ���<30���ӣ������ǰһ�ε�ҳ�������Ϣ
						if(timeDiff < 30*60*1000){
							v.set(session+"\00"+key.toString()+"\001"+beans.get(i-1).getRemote_user()
									+"\001"+beans.get(i-1).getTime_local()+"\001"+beans.get(i-1).getRequest()+"\001"
									+step+"\001"+(timeDiff/1000)+"\001"+beans.get(i-1).getHttp_referer()+"\001"+
									beans.get(i-1).getHttp_user_agent()+"\001"+beans.get(i-1).getBody_bytes_sent()+
									"\001"+beans.get(i-1).getStatus());
							context.write(NullWritable.get(), v);
							step++;
						}else{
							//����-�ϴ�ʱ���>30���ӣ������ǰһ�ε�ҳ�������Ϣ
							v.set(session+"\00"+key.toString()+"\001"+beans.get(i-1).getRemote_user()
									+"\001"+beans.get(i-1).getTime_local()+"\001"+beans.get(i-1).getRequest()+"\001"
									+step+"\001"+(60)+"\001"+beans.get(i-1).getHttp_referer()+"\001"+
									beans.get(i-1).getHttp_user_agent()+"\001"+beans.get(i-1).getBody_bytes_sent()+
									"\001"+beans.get(i-1).getStatus());
							context.write(NullWritable.get(), v);
							//�µ���һ�ν������¼�¼
							step = 1;
							session = UUID.randomUUID().toString();
						}
						//��������һ������ֱ���������
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
			
			FileInputFormat.setInputPaths(job, "F:/hadoop_file/���ǵ������־/one_process");
			FileOutputFormat.setOutputPath(job, new Path("F:/hadoop_file/���ǵ������־/pageViews/"));
			
			//��yarn��Ⱥ�ύ���job
			boolean res = job.waitForCompletion(true);
			System.exit(res ? 0:1);
			
		}
	}
	
}
