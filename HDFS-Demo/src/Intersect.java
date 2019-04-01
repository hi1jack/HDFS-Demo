import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Intersect {
	public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			context.write(value, new IntWritable(1));
		}
	}
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,NullWritable>{
		private FloatWritable result = new FloatWritable();
		public void reduce(Text key,Iterable<IntWritable> values,
				Context context)throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable val:values) {
				sum++;
			}
			NullWritable a = null;
			if(sum>=2) {
				context.write(key, a);
			}
		}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.128:9000");
		Job job = Job.getInstance(conf,"Intersection");
		
		job.setJarByClass(Intersect.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/data/data6"));
		FileOutputFormat.setOutputPath(job, new Path("/test2/rel03"));
		job.waitForCompletion(true);
	}
}
