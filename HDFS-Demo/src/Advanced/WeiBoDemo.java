package Advanced;

import java.io.DataInput;
import java.io.DataOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;



public class WeiBoDemo {
	//tab分隔符
	private static String TAB_SEPARATOR="\t";
	//粉丝
	private static String FAN = "fan";
	//关注
	private static String FOLLOWERS = "followers";
	//微博数
	private static String MICROBLOGS = "microblogs";
	
	public static class WeiBoMapper extends Mapper<Text, WeiBo, Text, Text>{
		
		protected void map(Text key,WeiBo value,Context context)throws IOException,
		InterruptedException{
			context.write(new Text(FAN), new Text(key.toString()+TAB_SEPARATOR+value.getFan()));
			
			context.write(new Text(FOLLOWERS), new Text(key.toString()+TAB_SEPARATOR+value.getFollowers()));
			
			context.write(new Text(MICROBLOGS), new Text(key.toString()+TAB_SEPARATOR+value.getMicroblogs()));
			
		}
	}
	
	public static class WeiBoReducer extends Reducer<Text, Text, Text, IntWritable> {
		private MultipleOutputs<Text, IntWritable> mos;
		
		protected void setup(Context context)throws IOException,InterruptedException{
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}
		protected void reduce(Text Key,Iterable<Text> Values,Context context)throws IOException,
		InterruptedException{
			Map<String,Integer> map = new HashMap<String,Integer>();
			for(Text value:Values) {
				//value=名称+（粉丝数或关注数或微博数）
				String[] records = value.toString().split(TAB_SEPARATOR);
				map.put(records[0],Integer.parseInt(records[1].toString()));
			}
			//对map内的数据进行排序
			Map.Entry<String,Integer>[] entries = getSortedHashtableByValue(map);
			
			for (int i = 0; i < entries.length; i++) {
				mos.write(Key.toString(), entries[i].getKey(), entries[i].getValue());
			}
		}
		protected void cleanup(Context context)throws IOException,InterruptedException {
			mos.close();
		}
	}
	
	
	public static Entry<String, Integer>[] getSortedHashtableByValue(Map<String, Integer> h) {
		// TODO Auto-generated method stub
		Entry<String,Integer>[] entries = (Entry<String, Integer>[])h.entrySet().toArray(new Entry[0]);
		//排序
		Arrays.sort(entries, new Comparator<Entry<String,Integer>>() {
			public int compare(Entry<String,Integer> entry1,Entry<String, Integer> entry2) {
				return entry2.getValue().compareTo(entry1.getValue());
			}
		});
		return entries;
	}
	
	
	public static class WeiBo implements WritableComparable<Object>{
		private int fan;
		private int followers;
		private int microblogs;
		public WeiBo() {}
		public WeiBo(int fan,int followers,int microblogs) {
			this.fan = fan;
			this.followers = followers;
			this.microblogs = microblogs;
		}
		
		public void set(int fan,int followers,int microblogs) {
			this.fan = fan;
			this.followers = followers;
			this.microblogs = microblogs;
		}
		
		//实现WritableComparable的readField()方法，以便该数据能被序列化后
		//完成网络传输或文件输入
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			fan = in.readInt();
			followers = in.readInt();
			microblogs = in.readInt();
		}
		
		//实现WritableComparable的readField()方法，以便该数据能被序列化后
		//完成网络传输或文件输出
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(fan);
			out.writeInt(followers);
			out.writeInt(microblogs);
		}

		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			return 0;
		}
		public int getFan() {
			return fan;
		}
		public void setFan(int fan) {
			this.fan = fan;
		}
		public int getFollowers() {
			return followers;
		}
		public void setFollowers(int followers) {
			this.followers = followers;
		}
		public int getMicroblogs() {
			return microblogs;
		}
		public void setMicroblogs(int microblogs) {
			this.microblogs = microblogs;
		}
	}
	public static class WeiboInputFormat extends FileInputFormat<Text, WeiBo>{

		@Override
		public RecordReader<Text, WeiBo> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return new WeiboRecordReader();
		}
		public class WeiboRecordReader extends RecordReader<Text, WeiBo>{
			public LineReader in;
			public Text lineKey = new Text();
			public WeiBo lineValue = new WeiBo();
			
			@Override
			public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, 
			InterruptedException {
				// TODO Auto-generated method stub
				FileSplit split = (FileSplit)input;
				Configuration job = context.getConfiguration();
				Path file = split.getPath();
				FileSystem fs = file.getFileSystem(job);
				FSDataInputStream filein = fs.open(file);
				in = new LineReader(filein,job);
			}
			
			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				Text line = new Text();
				int linesize = in.readLine(line);
				if(linesize == 0)
					return false;
				//通过分隔符“\t”将每行的数据解析成数组
				String[] pieces = line.toString().split("\t");
				if(pieces.length!=5) {
					throw new IOException("Invalid record received");
				}
				
				int a,b,c;
				try {
					//粉丝
					a = Integer.parseInt(pieces[2].trim());
					//关注
					b = Integer.parseInt(pieces[3].trim());
					//微博数
					c = Integer.parseInt(pieces[4].trim());
				}catch (NumberFormatException nfe) {
					// TODO: handle exception
					throw new IOException("Error parsing floating poing value in record");
				}
				//自定义key和value值
				lineKey.set(pieces[0]);
				lineValue.set(a, b, c);
				return true;
			}
			
			@Override
			public void close() throws IOException {
				// TODO Auto-generated method stub
				if (in != null) {
					in.close();
				}
			}

			@Override
			public Text getCurrentKey() throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				return lineKey;
			}

			@Override
			public WeiBo getCurrentValue() throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				return lineValue;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				return 0;
			}
			
		}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
		
		//判断路径是否存在，如存在则删除
		Path mypath = new Path("/data1/output8");
		FileSystem hdfs = mypath.getFileSystem(conf);
		if(hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		
		Job job = Job.getInstance(conf,"WeiBoDemo");
		job.setJarByClass(WeiBoDemo.class);
		
		job.setMapperClass(WeiBoMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(WeiBoReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/data1/data10/10.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/data1/output8"));
		job.setInputFormatClass(WeiboInputFormat.class);
		MultipleOutputs.addNamedOutput(job, FAN, TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, FOLLOWERS, TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, MICROBLOGS, TextOutputFormat.class, Text.class, IntWritable.class);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		job.waitForCompletion(true);
	}
	
}
