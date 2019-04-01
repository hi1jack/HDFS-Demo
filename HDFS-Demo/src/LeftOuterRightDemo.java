import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LeftOuterRightDemo {
	public static class MyMapper extends Mapper<Object,Text,Text,Text>{
		
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String child = value.toString().split("\t")[0];
			String parent = value.toString().split("\t")[1];
			//产生正序和逆序的key-value，同时压入context
			context.write(new Text(child), new Text("-"+parent));
			context.write(new Text(parent),new Text("+"+child));
		}
	}
	
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key,Iterable<Text> values,
				Context context)throws IOException,InterruptedException{
			ArrayList<Text> grandparent = new ArrayList<Text>();
			ArrayList<Text> grandchild = new ArrayList<Text>();
			for(Text t:values) {//对各个value中的值进行处理
				String s = t.toString();
				if(s.startsWith("-")) {
					grandparent.add(new Text(s.substring(1)));
				}else {
					grandchild.add(new Text(s.substring(1)));
				}
			}
			//再将grandparent与grandchild中的东西一一对应输出
			for(int i=0;i<grandchild.size();i++) {
				for(int j=0;j<grandparent.size();j++) {
					context.write(grandchild.get(i),grandparent.get(j));
				}
			}			
		}
	}
	
	
	
	
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.128:9000");
		
		Job job = Job.getInstance(conf,"LeftOuterRightDemo");
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("/data/data8"));
		FileOutputFormat.setOutputPath(job, new Path("/test2/output01"));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
