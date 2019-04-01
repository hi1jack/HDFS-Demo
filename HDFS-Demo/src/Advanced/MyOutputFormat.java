package Advanced;

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;



public class MyOutputFormat {
	public static class MyMapper extends Mapper<LongWritable, Text,Text, LongWritable>{
		private Text word = new Text();
		private LongWritable writable = new LongWritable();
		
		protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, Text, 
				LongWritable>.Context context )throws IOException,InterruptedException{
			if(value != null) {
				String line = value.toString();
				StringTokenizer tokenizer = new StringTokenizer(line);
				while(tokenizer.hasMoreTokens()) {
					word.set(tokenizer.nextToken());
					context.write(word, writable);
				}
			}
		}
	}
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		protected void reducer(Text key,Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
		throws IOException,InterruptedException{
			long sum = 0;
			for(LongWritable value:values) {
				sum +=value.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
	public static class MyselfOutputFormat extends OutputFormat<Text,LongWritable>{
		private FSDataOutputStream outputStream = null;

		@Override
		public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return new FileOutputCommitter(new Path("/data1"), context);
		}

		@Override
		public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
				FileSystem fileSystem = FileSystem.get(new URI("/data1/output3"),
						context.getConfiguration());
				final Path path = new Path("/data1/output3/output");
				this.outputStream = fileSystem.create(path,false);
			}catch (URISyntaxException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			return new MyRecorderWriter(outputStream);
		}
		
	}
	public static class MyRecorderWriter extends RecordWriter<Text, LongWritable>{
		private FSDataOutputStream outputStream = null;
		public MyRecorderWriter(FSDataOutputStream outputStream) {
			this.outputStream = outputStream;
		}
		@Override
		public void close(org.apache.hadoop.mapreduce.TaskAttemptContext arg0)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			this.outputStream.close();
		}

		@Override
		public void write(Text key, LongWritable value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			this.outputStream.writeBytes(key.toString());
			this.outputStream.writeBytes("\t");
			this.outputStream.writeLong(value.get());
		}
		
	}
	public static void main(String[] args)throws IOException,URISyntaxException,
	ClassNotFoundException,InterruptedException{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
		FileSystem fileSystem = FileSystem.get(new URI("/data1/output3"),conf);
		fileSystem.delete(new Path("/data1/output3"), true);
		
		Job job = Job.getInstance(conf,"MyOutputFormat");
		job.setJarByClass(MyOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("/data1/data4/4.txt"));
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(MyselfOutputFormat.class);
		
		job.waitForCompletion(true);
	}
}
