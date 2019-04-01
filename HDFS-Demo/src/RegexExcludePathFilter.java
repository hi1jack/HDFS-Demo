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
	//通配符的使用
	public static void list()throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.128:9000" );
		System.setProperty("HADOOP_USER_NAME", "root");
		FileSystem fs = FileSystem.get(conf);
		//PathFilter是过滤不符合置顶表达式的路径，下列就是把以txt结尾的过滤掉
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
