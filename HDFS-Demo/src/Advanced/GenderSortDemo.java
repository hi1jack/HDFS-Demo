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
		 * ����map����һ�����ݣ��������ݴ洢��value�����У�Ȼ����ݡ�\t���ָ���������
		 * ���������䣬�Ա�ͳɼ�
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
	 * �ϲ�mapper������
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
	 * ����age�ν�map���������ȷֲ���reduce��
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
		 * ͳ�Ƴ���ͬ�Ա����߷���
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
			//��ȡ�����ļ�
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS","hdfs://192.168.254.128:9000");
			System.setProperty("HADOOP_USER_NAME", "root");
			
			Path myPath = new Path("/data1/output9");
			FileSystem hdfs = myPath.getFileSystem(conf);
			if(hdfs.isDirectory(myPath)) {
				hdfs.delete(myPath,true);	
			}
			//�½�һ������
			Job job = Job.getInstance(conf,"GenderSortDemo");
			//����
			job.setJarByClass(GenderSortDemo.class);
			
			job.setMapperClass(GenderMapper.class);
			job.setReducerClass(GenderReducer.class);
			//map���key����
			job.setMapOutputKeyClass(Text.class);
			//map���value����
			job.setMapOutputValueClass(Text.class);
			//reduce���key����
			job.setOutputKeyClass(Text.class);
			//reduce���value����
			job.setOutputValueClass(Text.class);
			
			//����combiner��
			job.setCombinerClass(GenderCombiner.class);
			//����partitioner��
			job.setPartitionerClass(GenderPartitioner.class);
			//reduce��������Ϊ3
			job.setNumReduceTasks(3);
			//�������·��
			FileInputFormat.addInputPath(job, new Path("/data1/data11/"));
			FileOutputFormat.setOutputPath(job, new Path("/data1/output9"));
			//�ύ����
			job.waitForCompletion(true);
		}
	}
}
