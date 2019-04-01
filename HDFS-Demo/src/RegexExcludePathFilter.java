import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexExcludePathFilter implements PathFilter {
	private final String regex;
	public RegexExcludePathFilter(String regex) {
		this.regex = regex;
	}
	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		return !path.toString().matches(regex);
	}
	//ͨ�����ʹ��
	public static void list()throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.128:9000" );
		System.setProperty("HADOOP_USER_NAME", "root");
		FileSystem fs = FileSystem.get(conf);
		//PathFilter�ǹ��˲������ö����ʽ��·�������о��ǰ���txt��β�Ĺ��˵�
		FileStatus[] status = fs.globStatus(new Path("/test/*"),
				new RegexExcludePathFilter(".*txt"));
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for(Path p :listedPaths) {
			System.out.println(p);
		}
	}
public static void main(String[] args)throws IllegalArgumentException,Exception {
	list();
}
}
