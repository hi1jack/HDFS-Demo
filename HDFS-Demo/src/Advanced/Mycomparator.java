package Advanced;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Mycomparator {
	public static class MySort implements WritableComparable<MySort>{
		private String first;
		private int second;
		public  MySort() {		}
		public  MySort(String first,int second) {
			this.first = first;
			this.second = second;
		}
		/*
		 * 反序列化，从流中的二进制转化成自定义key
		 */
		@Override
		public void readFields(DataInput input) throws IOException {
			// TODO Auto-generated method stub
			this.first = input.readUTF();
			this.second = input.readInt();
		}
		/*
		 *序列化，将自定义key转换成使用流传送的二进制
		 */
		@Override
		public void write(DataOutput output) throws IOException {
			// TODO Auto-generated method stub
			output.writeUTF(first);
			output.writeInt(second);
		}
		public String getFirst() {
			return first;
		}
		public void setFirst(String first) {
			this.first = first;
		}
		public int getSecond() {
			return second;
		}
		public void setSecond(int second) {
			this.second = second;
		}
		@Override
		public int compareTo(MySort o) {
			// TODO Auto-generated method stub
			return 0;
		}
		
	}
	public static class SortComparator extends WritableComparator{
		public SortComparator() {
			super(MySort.class,true);
		}
		public int compare(WritableComparable a,WritableComparable b) {
			MySort a1 = (MySort)a;
			MySort b1 = (MySort)b;
			if(!a1.getFirst().equals(b1.getFirst())) {
				return a1.getFirst().compareTo(b1.getFirst());
			}else {
				return -(a1.getSecond()-b1.getSecond());
			}	
		}
	}
	public static class GroupingComparator extends WritableComparator{
		public GroupingComparator() {
			super(MySort.class,true);
		}
		public int compare(WritableComparable a,WritableComparable b) {
			MySort a1 = (MySort)a;
			MySort b1 = (MySort)b;
			return a1.getFirst().compareTo(b1.getFirst());
		}
	}
	public static class MyMapper extends Mapper<LongWritable, Text, MySort, IntWritable>{
		private IntWritable cost = new IntWritable();
		protected void map(LongWritable key,Text value,Context context)
					throws IOException,InterruptedException{
			String line = value.toString().trim();
			if(line.length()>0) {
				String[] arr = line.split(",");
				if(arr.length == 3) {
					cost.set(Integer.valueOf(arr[2]));
					context.write(new MySort(arr[1],Integer.valueOf(arr[2])), cost);
				}
			}
		}
	}
	public static class MyReducer extends Reducer<MySort, IntWritable, Text, Text>{
		private Text okey = new Text();
		private Text ovalue = new Text();
		protected void reduce(MySort key,Iterable<IntWritable> values,Context context)
					throws IOException,InterruptedException{
			StringBuffer sb = new StringBuffer();
			/*
			 * 把同一用户的消费情况进行拼接
			 */
			for(IntWritable value:values ) {
				sb.append(",");
				sb.append(value.get());
			}
			sb.delete(0, 1);
			okey.set(key.getFirst());
			ovalue.set(sb.toString());
			context.write(okey, ovalue);
		}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME","root");
		Job job = Job.getInstance(conf,"MyComparator");
		job.setJarByClass(Mycomparator.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(MySort.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/data1/data8"));
		Path outputDir = new Path("/data1/output6");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		job.waitForCompletion(true);
	}
}
