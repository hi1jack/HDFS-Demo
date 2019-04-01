import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class Sort {
	//map将输入中的value化成IntWritable类型，作为输出的key
	public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
		private static IntWritable data = new IntWritable();
		//实现map函数
		public void map(Object key,Text value,Context context)
				throws IOException,InterruptedException{
			String line=value.toString();
			data.set(Integer.parseInt(line));
			context.write(data,new IntWritable(1));
		}
	}
	//reduce将输入中的key复制到输出元素的key上
	//然后根据输入的value-list中的元素的个数决定key的输出次数
	//利用了shuffle对key的排序原理，故map中的key为数据
	public static class Reduce extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		private static IntWritable linenum = new IntWritable(1);
		private static int i=1;
		//实现reduce函数
		public void reduce(IntWritable key,Iterable<IntWritable>values,Context context)
				throws IOException,InterruptedException{
			
			context.write(new IntWritable(i),key);
			++i;
		}  
	}
	
	public static void main(String[] args)throws Exception {
	Configuration conf = new Configuration();
	conf.set("fs.defaultFS","hdfs://192.168.254.128:9000");
	Job job = Job.getInstance(conf,"sort");
	job.setJarByClass(Sort.class);
	//设置Map和Reduce处理类
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	//设置输出类型
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);
	//设置输入和输出目录
	FileInputFormat.addInputPath(job, new Path("/data/data4"));
	FileOutputFormat.setOutputPath(job, new Path("/test2/sort01"));
	System.exit(job.waitForCompletion(true)?0:1);
	}
}
