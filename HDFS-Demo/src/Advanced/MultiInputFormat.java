package Advanced;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MultiInputFormat {
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,Context context)
				throws IOException,InterruptedException {
			int sum = 0;
			for(IntWritable val:values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
		Job job = Job.getInstance(conf,"word count");
		//打包运行必须执行的方法
		job.setJarByClass(MultiInputFormat.class);
		
		Path path1 = new Path("/data/data1/data1/1.txt");
		Path path2 = new Path("/data/data1/data2/2.txt");
		MultipleInputs.addInputPath(job, path1, TextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(job, path2, TextInputFormat.class, Mapper1.class);
		
		job.setCombinerClass(IntSumReducer.class);
		
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path("/data/data1/output1"));
		job.waitForCompletion(true);
		
	}
}
