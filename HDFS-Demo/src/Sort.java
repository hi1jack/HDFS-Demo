import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class Sort {
	//map�������е�value����IntWritable���ͣ���Ϊ�����key
	public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
		private static IntWritable data = new IntWritable();
		//ʵ��map����
		public void map(Object key,Text value,Context context)
				throws IOException,InterruptedException{
			String line=value.toString();
			data.set(Integer.parseInt(line));
			context.write(data,new IntWritable(1));
		}
	}
	//reduce�������е�key���Ƶ����Ԫ�ص�key��
	//Ȼ����������value-list�е�Ԫ�صĸ�������key���������
	//������shuffle��key������ԭ����map�е�keyΪ����
	public static class Reduce extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		private static IntWritable linenum = new IntWritable(1);
		private static int i=1;
		//ʵ��reduce����
		public void reduce(IntWritable key,Iterable<IntWritable>values,Context context)
				throws IOException,InterruptedException{
			
			context.write(new IntWritable(i),key);
			++i;
		}  
	}
	
	public static void main(String[] args)throws Exception {
	Configuration conf = new Configuration();
	conf.set("fs.defaultFS","hdfs://192.168.254.128:9000");
	Job job = Job.getInstance(conf,"sort");
	job.setJarByClass(Sort.class);
	//����Map��Reduce������
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	//�����������
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);
	//������������Ŀ¼
	FileInputFormat.addInputPath(job, new Path("/data/data4"));
	FileOutputFormat.setOutputPath(job, new Path("/test2/sort01"));
	System.exit(job.waitForCompletion(true)?0:1);
	}
}
