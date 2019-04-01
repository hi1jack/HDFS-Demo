import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordDelete {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text word =new Text();
		
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				
				context.write(word,one);
			}
		}
	}
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,
				Context context)throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable val:values) {
				sum += val.get();
			}
			result.set(sum);
			NullWritable a = null;
			context.write(key,a);
		}
	}
	public static void main(String[] args)throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		
		Job job = Job.getInstance(conf,"word delete");
		job.setJarByClass(WordDelete.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(cls);
		job.setReducerClass(IntSumReducer.class);
		
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/data/data1/1.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/test2/wd01"));
		job.waitForCompletion(true);
	}
}
