package Advanced;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import sun.security.ssl.KerberosClientKeyExchange;

public class EmailMultipleOutputsDemo {
	public static class EmailMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		protected void map(LongWritable key,Text value,Context context)throws IOException,
		InterruptedException {
			context.write(value, one);
		}
	}
	public static class EmailReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private MultipleOutputs<Text, IntWritable> multipleOutputs;
		protected void setup(Context context)throws IOException,InterruptedException{
			multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
		}
		protected void reduce(Text Key,Iterable<IntWritable> Values,Context context)throws
		IOException,InterruptedException{
			int begin = Key.toString().indexOf("@");
			int end = Key.toString().indexOf(".");
			if(begin >= end) {
				return;
			}
			String name = Key.toString().substring(begin+1, end);
			int sum = 0;
			for(IntWritable value:Values) {
				sum += value.get();
			}
			multipleOutputs.write(Key, new IntWritable(sum), name);
		}
		protected void cleanup(Context context)throws IOException,InterruptedException {
			multipleOutputs.close();
		}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
		
		Path mypath = new Path("/data1/output5");
		FileSystem hdfs = mypath.getFileSystem(conf);
		if(hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf,"EmailMultipleOutputsDemo");
		job.setJarByClass(EmailMultipleOutputsDemo.class);
		
		FileInputFormat.setInputPaths(job, new Path("/data1/data6"));
		FileOutputFormat.setOutputPath(job, new Path("/data1/output5"));
		job.setMapperClass(EmailMapper.class);
		job.setReducerClass(EmailReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.waitForCompletion(true);
	}
}
