package Advanced;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class ScoreStats {
	
	public static class StudentPartitioner extends Partitioner<IntWritable, Text>{

		@Override
		public int getPartition(IntWritable key, Text value, int numReduceTasks) {
			// TODO Auto-generated method stub
			int scoreInt = key.get();
			
			if(numReduceTasks == 0) 
				return 0;
			if(scoreInt < 60) {
				return 0;
			}
			else if (scoreInt <= 80) {
				return 1;
			}else {
				return 2;
			}
		}
	}
	public static class StudentMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		protected void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			String[] studentArr = value.toString().split("\t");
			
			if(StringUtils.isNotBlank(studentArr[1])) {
				/*
				 * 姓名	成绩（中间以tab分隔）
				 *张三	85 
				 */
				IntWritable pKey = new IntWritable(Integer.parseInt(studentArr[1].trim()));
				context.write(pKey,	value);
			}
		}
	}
	public static class StudentReducer extends Reducer<IntWritable, Text, NullWritable, Text>{
		@Override
		protected void reduce(IntWritable key,Iterable<Text> values,Context context)
				throws IOException,InterruptedException {
			for(Text value:values) {
				context.write(NullWritable.get(),value);
			}
		}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
		
		Job job = Job.getInstance(conf,"Score stats");
		//打包运行必须执行的方法
		job.setJarByClass(StudentPartitioner.class);
		
		FileInputFormat.addInputPath(job, new Path("/data/data1/data3"));
		FileOutputFormat.setOutputPath(job, new Path("/data/data1/output2"));
		
		job.setMapperClass(StudentMapper.class);
		job.setReducerClass(StudentReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(StudentPartitioner.class);
		job.setNumReduceTasks(3);
		
		job.waitForCompletion(true);
		
	}
}
