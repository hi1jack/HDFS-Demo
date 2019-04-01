import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Select {
	public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String[] str = value.toString().split("\t");
			context.write(new Text(str[1]), new IntWritable(1));
		}
	}
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,NullWritable>{
		private FloatWritable result = new FloatWritable();
		public void reduce(Text key,Iterable<IntWritable> values,
				Context context)throws IOException,InterruptedException{
			NullWritable a = null;
			context.write(key, a);
		}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		Job job = Job.getInstance(conf,"Select");
		
		job.setJarByClass(Select.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
	   
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/data/data6"));
		FileOutputFormat.setOutputPath(job, new Path("/test2/rel01"));
		job.waitForCompletion(true);
	}
}
