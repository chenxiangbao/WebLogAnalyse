package cn.itcast.bidData.WebLogAnalyse.one;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Set;

/*
0 58.215.204.118 
1 - 
2 - 
3 [18/Sep/2013:06:51:36 
4 +0000] 
5 "GET 
6 /wp-content/uploads/2013/08/chat2.png 
7 HTTP/1.1" 
8 200 
9 59852 
10 "http://blog.fens.me/nodejs-socketio-chat/" 
11 "Mozilla/5.0 (Windows NT 5.1; rv:23.0) Gecko/20100101 Firefox/23.0"
*/
public class WebLogParser {
	 public static SimpleDateFormat  df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
	 public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.US);
	 
	 public static WebLogBean parser(String line){
		 WebLogBean webLogBean = new WebLogBean();
		 String[] arr = line.split(" ");
		 if(arr.length>11){
			 webLogBean.setRemote_addr(arr[0]);
			 webLogBean.setRemote_user(arr[1]);
			 String time_local = formateDate(arr[3].substring(1));
			 if(null==time_local) time_local="-invalid_time-";
			 webLogBean.setTime_local(time_local);
			 webLogBean.setRequest(arr[6]);
			 webLogBean.setStatus(arr[8]);
			 webLogBean.setBody_bytes_sent(arr[9]);
			 webLogBean.setHttp_referer(arr[10]);
			 
			 //如果useragent元素较多，拼接useragent
			 if(arr.length>12){
				 StringBuilder sb = new StringBuilder();
				 for(int i =11;i<arr.length;i++){
					 sb.append(arr[i]);
				 }
				 webLogBean.setHttp_user_agent(sb.toString());
			 }else{
				 webLogBean.setHttp_user_agent(arr[11]);
			 }
			 
			 if(Integer.parseInt(webLogBean.getStatus())>=400){
				 //大于400 http错误
				 webLogBean.setValid(false);
			 }
			 
			 if("-invalid_time-".equals(webLogBean.getTime_local())){
				 webLogBean.setValid(false);
			 }
			 
		 }else{
			 webLogBean.setValid(false);
		 }
		 
		 return webLogBean;
	 }

	private static String formateDate(String time_local) {
		try {
			return df2.format(df1.parse(time_local));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
	 
	public static void filtStaticResource(WebLogBean bean,Set<String> pages){
		if(!pages.contains(bean.getRequest())){
			bean.setValid(false);
		}
	}
}
