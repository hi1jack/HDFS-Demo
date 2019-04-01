

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScoreAverage {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
			String[] str = value.toString().split("\t");
			context.write(new Text(str[0]), new IntWritable(Integer.parseInt(str[1])));
		}
	}
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, FloatWritable>{
		private FloatWritable result = new FloatWritable();
		public void reduce(Text key,Iterable<IntWritable> values,
				Context context
				)throws IOException,InterruptedException {
			int sum = 0;
			for(IntWritable val:values) {
				sum +=val.get();
			}
			result.set((float)sum/3);
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		Job job = Job.getInstance(conf,"ScoreAverage");
		
		job.setJarByClass(ScoreAverage.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/data/data3"));
		FileOutputFormat.setOutputPath(job, new Path("/test2/sc01"));
		job.waitForCompletion(true);
	}
}
