package Advanced;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class MysqlDBOutputormatDemo {
	/*
	 * 实现DBWritable
	 * 
	 * Tbls Writable需要向Mysql中写入数据
	 */
	public static class TblsWritable implements Writable,DBWritable{
		String tbl_name;
		int tbl_age;
		public TblsWritable() {
			// TODO Auto-generated constructor stub
		}
		public TblsWritable(String name,int age) {
			this.tbl_name = name;
			this.tbl_age = age;
		}
		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			// TODO Auto-generated method stub
			this.tbl_name = resultSet.getString(1);
			this.tbl_age = resultSet.getInt(2);
		}

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			// TODO Auto-generated method stub
			statement.setString(1, this.tbl_name);
			statement.setInt(2, this.tbl_age);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.tbl_name = in.readUTF();
			this.tbl_age = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(this.tbl_name);
			out.writeInt(this.tbl_age);
		}
		public String toString() {
			return new String(this.tbl_name+""+this.tbl_age);
			
		}
	}
	public static class StudentMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		protected void map(LongWritable key,Text value,Context context)throws IOException,
		InterruptedException{
			context.write(key, value);
		}
	}
	public static class StudentReducer extends Reducer<LongWritable, Text, TblsWritable, TblsWritable>{
		protected void reduce(LongWritable key,Iterable<Text> values,Context context)throws
		IOException,InterruptedException{
			StringBuilder value = new StringBuilder();
			for(Text text:values) {
				value.append(text);
			}
			String[] studentArr = value.toString().split("\t");
			
			if(StringUtils.isNotBlank(studentArr[0])) {
				String name = studentArr[0].trim();
				
				int age = 0;
				try {
					age = Integer.parseInt(studentArr[1].trim());
				}catch (NumberFormatException e) {
					// TODO: handle exception
				}
				context.write(new TblsWritable(name, age), null);
			}
		}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME","root");
		
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", 
				"jdbc:mysql://192.168.254.128:3306/hadoop", "root", "Aa123456@");
		
		Job job = Job.getInstance(conf,"MysqlDBOutputormatDemo");
		job.setJarByClass(MysqlDBOutputormatDemo.class);
		FileInputFormat.addInputPath(job, new Path("/data1/data7/student.txt"));
		job.setMapperClass(StudentMapper.class);
		job.setReducerClass(StudentReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);
		
		DBOutputFormat.setOutput(job,"student","name","age");
		
		//job.addArchiveToClassPath(new Path("/lib/mysql/mysql-connector-java-5.1.47-bin.jar"));
		
		job.waitForCompletion(true);
	}
}
