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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Minus {
	public static class TokenizerMapper extends Mapper<Object,Text,Text,Text>{
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			FileSplit file = (FileSplit)context.getInputSplit();
			String fileName = file.getPath().getName();
			context.write(value, new Text(fileName));
		}
	}
	public static class IntSumReducer extends Reducer<Text,Text,Text,NullWritable>{
		private FloatWritable result = new FloatWritable();
		public void reduce(Text key,Iterable<Text> values,
				Context context)throws IOException,InterruptedException{
			int flag = 0;
			for(Text val:values) {
				if("B.txt".equals(val.toString())) {
					flag = 1;
					break;
				}
			}
			
			if(flag==0) {
				NullWritable a =null;
				context.write(key, a);
			}
		}
	}
	
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.128:9000");
		Job job = Job.getInstance(conf,"minus");
		
		job.setJarByClass(Minus.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/data/data6"));
		FileOutputFormat.setOutputPath(job, new Path("/test2/rel04"));
		job.waitForCompletion(true);
	}
}
