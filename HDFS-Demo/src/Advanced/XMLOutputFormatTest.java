package Advanced;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class XMLOutputFormatTest extends Configured implements Tool{
	
	public static class XMLOutputFormat extends FileOutputFormat<LongWritable, Text>{
		
		protected static class XMLRecordWriter extends RecordWriter<LongWritable, Text>{
			private DataOutputStream out;
			public XMLRecordWriter(DataOutputStream out)throws IOException {
				// TODO Auto-generated constructor stub
				this.out = out;
				out.writeBytes("<xml>\n");
			}
			private void writeTag(String tag,String value)throws IOException {
				// TODO Auto-generated method stub
				if("content".equals(tag))
					out.writeBytes("<"+tag+">"+value+"</"+tag+">\n");
				else 
					out.writeBytes("<"+tag+">"+value+"</"+tag+">\n\t");
			}
			@Override
			public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				try {
					out.writeBytes("</xml>\n");
				}finally {
					out.close();
				}
			}

			@Override
			public void write(LongWritable key, Text value) throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				out.writeBytes("<data>\n\t");
				this.writeTag("key", Long.toString(key.get()));
				String contents[] = value.toString().split(",");
				this.writeTag("content", contents[0]);
				out.writeBytes("</data>\n");
			}
		}
		@Override
		public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Path file = getDefaultWorkFile(job, "");
			FileSystem fs = file.getFileSystem(job.getConfiguration());
			FSDataOutputStream fileout = fs.create(file, false);
			return new XMLRecordWriter(fileout);
		}
		
	}
	public static class TextToXMLConversionMapper extends Mapper<LongWritable, Text,
	LongWritable, Text>{
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException {
			if(value.toString().contains(" "))
				context.write(key, value);
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf());
		job.setJarByClass(XMLOutputFormatTest.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(XMLOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(TextToXMLConversionMapper.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return job.isSuccessful()?0:1;
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME","root");
		
		Job job = Job.getInstance(conf,"XMLOutputFormat");
		job.setJarByClass(XMLOutputFormatTest.class);
		//job.setJarByClass(XMLOutputFormatTest.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(XMLOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(TextToXMLConversionMapper.class);
		FileInputFormat.setInputPaths(job, new Path("/data1/data5"));
		FileOutputFormat.setOutputPath(job, new Path("/data1/output4"));
		job.waitForCompletion(true);
	}
}
