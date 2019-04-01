import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Temperature {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
			String year = value.toString().substring(0,4);
			int temp = Integer.parseInt(value.toString());
			context.write(new Text(year),new IntWritable(temp));
			System.out.println(temp);
		}
	}
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,
				Context context) throws IOException,InterruptedException{
		int max = Integer.MIN_VALUE;
		for(IntWritable val:values) {
			if(max<val.get())
				max = val.get();
		}
		context.write(key, new IntWritable(max));
		}
	}
	public static void main(String[] args)throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		
		Job job = Job.getInstance(conf,"word count");
		
		job.setJarByClass(Temperature.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.addInputPath(job,new Path("/data/data5"));
		FileOutputFormat.setOutputPath(job,new Path("/test2/tt01"));
		job.waitForCompletion(true);
		
	}
}
