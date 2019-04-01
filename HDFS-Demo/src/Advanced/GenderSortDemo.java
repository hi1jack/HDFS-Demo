package Advanced;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenderSortDemo {
	private static String TAB_SEPARATOR = "\t";
	
	public static class GenderMapper extends Mapper<LongWritable, Text, Text, Text>{
		/*
		 * 调用map解析一行数据，该行数据存储在value参数中，然后根据“\t”分隔符解析出
		 * 姓名，年龄，性别和成绩
		 */
		public void map(LongWritable key,Text value,Context context)throws IOException,
		InterruptedException{
			String[] tokens = value.toString().split(TAB_SEPARATOR);
			String gender = tokens[2];
			String nameAgeScore = tokens[0] + TAB_SEPARATOR + tokens[1] + TAB_SEPARATOR +
					tokens[3];
			context.write(new Text(gender), new Text(nameAgeScore));
		}
	}
	/*
	 * 合并mapper输出结果
	 */
	public static class GenderCombiner extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,
		InterruptedException{
			int maxScore = Integer.MIN_VALUE;
			int score = 0;
			String name = " ";
			String age = " ";
			
			for(Text val:values) {
				String[] valTokens = val.toString().split(TAB_SEPARATOR);
				score = Integer.parseInt(valTokens[2]);
				if(score > maxScore) {
					name = valTokens[0];
					age = valTokens[1];
					maxScore = score;
				}
			}
			context.write(key, new Text(name + TAB_SEPARATOR + age + TAB_SEPARATOR + 
					maxScore));
		}
	}
	/*
	 * 根据age段将map输出结果均匀分布在reduce上
	 */
	public static class GenderPartitioner extends Partitioner<Text, Text>{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			// TODO Auto-generated method stub
			String[] nameAgeScore = value.toString().split(TAB_SEPARATOR);
			int age = Integer.parseInt(nameAgeScore[1]);
			if(numReduceTasks == 0)
				return 0;
			if(age <= 20) {
				return 0;
			}else if (age <= 50) {
				return 1 % numReduceTasks;
			}else {
				return 2 % numReduceTasks;
			}
		}
		/*
		 * 统计出不同性别的最高分数
		 */
		public static class GenderReducer extends Reducer<Text, Text, Text, Text>{
			public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,
			InterruptedException{
				int maxScore = Integer.MIN_VALUE;
				int score = 0;
				String name = " ";
				String age = " ";
				String gender = " ";
				
				for(Text val:values) {
					String[] valTokens = val.toString().split(TAB_SEPARATOR);
					score = Integer.parseInt(valTokens[2]);
					if(score > maxScore) {
						name = valTokens[0];
						age = valTokens[1];
						gender = key.toString();
						maxScore = score;
					}
				}
				context.write(new Text(name), new Text("age:" + age + TAB_SEPARATOR + "gender:" +
				gender + TAB_SEPARATOR + "score:" + maxScore));
			}
		}
		public static void main(String[] args)throws Exception {
			//读取配置文件
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS","hdfs://192.168.254.128:9000");
			System.setProperty("HADOOP_USER_NAME", "root");
			
			Path myPath = new Path("/data1/output9");
			FileSystem hdfs = myPath.getFileSystem(conf);
			if(hdfs.isDirectory(myPath)) {
				hdfs.delete(myPath,true);	
			}
			//新建一个任务
			Job job = Job.getInstance(conf,"GenderSortDemo");
			//主类
			job.setJarByClass(GenderSortDemo.class);
			
			job.setMapperClass(GenderMapper.class);
			job.setReducerClass(GenderReducer.class);
			//map输出key类型
			job.setMapOutputKeyClass(Text.class);
			//map输出value类型
			job.setMapOutputValueClass(Text.class);
			//reduce输出key类型
			job.setOutputKeyClass(Text.class);
			//reduce输出value类型
			job.setOutputValueClass(Text.class);
			
			//设置combiner类
			job.setCombinerClass(GenderCombiner.class);
			//设置partitioner类
			job.setPartitionerClass(GenderPartitioner.class);
			//reduce个数设置为3
			job.setNumReduceTasks(3);
			//输入输出路径
			FileInputFormat.addInputPath(job, new Path("/data1/data11/"));
			FileOutputFormat.setOutputPath(job, new Path("/data1/output9"));
			//提交任务
			job.waitForCompletion(true);
		}
	}
}
