package cn.itcast.bidData.WebLogAnalyse.three;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

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

import cn.itcast.bidData.WebLogAnalyse.two.PageViewsBean;


/*
 * �������ݸ�ʽ
7765a1d8-f595-451c-9b4e-0bd1a049b433 
183.60.177.228
-
2013-09-18 09:08:04
/finance-rhive-repurchase/
3
45
"http://redir.yy.duowan.com/redir.php?url=http%3A%2F%2Fblog.fens.me%2Ffinance-rhive-repurchase%2F"
"Mozilla/5.0(compatible;MSIE9.0;WindowsNT6.1;WOW64;Trident/5.0)"
11895
200
*/
public class ClickStreamVisit {
	//��session��Ϊkey���������ݵ�reducer
	static class ClickStreamVisitMapper extends Mapper<LongWritable, Text, Text, PageViewsBean>{
		PageViewsBean pvBean = new PageViewsBean();		
		Text k  = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] filelds = line.split("\001");
			
			int step = Integer.parseInt(filelds[5]);
			
			pvBean.set(filelds[0], filelds[1], filelds[2], filelds[3], 
					filelds[4], step, filelds[6], filelds[7], 
					filelds[8], filelds[9]);
			
			k.set(pvBean.getSession());
			context.write(k, pvBean);
		}		
	}
	//��ͬһsession�µķ��ʰ�stepȡ����β��ƴ�ӵ�һ���ֶ�
	static class ClickStreamVisitReducer extends Reducer<Text, PageViewsBean, NullWritable, VisitBean>{

		@Override
		protected void reduce(Text session,Iterable<PageViewsBean> pvBeans,Context context)
				throws IOException, InterruptedException {
			//��pvBean����step����
			ArrayList<PageViewsBean> pvBeansList = new ArrayList<PageViewsBean>();			
			for(PageViewsBean pvBean : pvBeans){
				PageViewsBean bean = new PageViewsBean();
				try {
					BeanUtils.copyProperties(bean, pvBean);
					pvBeansList.add(bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			Collections.sort(pvBeansList,new Comparator<PageViewsBean>() {
				@Override
				public int compare(PageViewsBean o1, PageViewsBean o2) {
					return o1.getStep()>o2.getStep() ? 1 : -1;
				}			
			});
			
			//ȡ���visit����βpageView��¼�������ݷ���VisitBean��
			VisitBean visitBean = new VisitBean();
			//ȡxisit���׼�¼
			visitBean.setInPage(pvBeansList.get(0).getRequest());
			visitBean.setInTime(pvBeansList.get(0).getTimestr());
			//ȡvisit��β��¼
			visitBean.setOutPage(pvBeansList.get(pvBeansList.size()-1).getRequest());
			visitBean.setOutTime(pvBeansList.get(pvBeansList.size()-1).getTimestr());
			//visit���ʵ�ҳ����
			visitBean.setPageVisits(pvBeansList.size());
			//�����ߵ�ip
			visitBean.setRemote_addr(pvBeansList.get(0).getRemote_addr());
			//����visit��referal
			visitBean.setReferal(pvBeansList.get(0).getReferal());
			visitBean.setSession(session.toString());
			context.write(NullWritable.get(), visitBean);
			
		}	
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ClickStreamVisit.class);
		job.setMapperClass(ClickStreamVisitMapper.class);
		job.setReducerClass(ClickStreamVisitReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageViewsBean.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VisitBean.class);
		
		FileInputFormat.setInputPaths(job, "F:/hadoop_file/���ǵ������־/two_pageViews");
		FileOutputFormat.setOutputPath(job, new Path("F:/hadoop_file/���ǵ������־/visit/"));
		
		//��yarn��Ⱥ�ύ���job
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0:1);
	}
}
