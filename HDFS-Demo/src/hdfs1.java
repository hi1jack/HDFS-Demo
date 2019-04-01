import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.fs.Path;

public class hdfs1 {
	/**
	 * 列出所有Datanode的名字
	 */	
public static void listDataNodeInfo() {
	try {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
			FileSystem fS = FileSystem.get(conf);
			DistributedFileSystem hdfs = (DistributedFileSystem)fS;
			DatanodeInfo[] dataNodestats = hdfs.getDataNodeStats();
			String[] names = new String[dataNodestats.length];
			System.out.println("List of all the datanodes in the HDFS cluster.");
			
			for (int i = 0; i < names.length; i++) {
				names[i] = dataNodestats[i].getHostName();
				System.out.println(names[i]);
			}
			System.out.println(hdfs.getUri().toString());
	} catch (Exception e) {
		// TODO: handle exception
		e.printStackTrace();
	}
}
/**
 * 创建文件目录
 * @param Path:创建的文件名
 */
public static void mkdir(String path)throws IOException {
	Configuration conf = new Configuration();
	conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
	System.setProperty("HADOOP_USER_NAME", "root");
	FileSystem fs =FileSystem.get(conf);
	Path srcPath = new Path(path);
	boolean isok = fs.mkdirs(srcPath);
	if (isok) {
		System.out.println("create dir ok!");
	}else {
		System.out.println("create dir failure!");
	}
	fs.close();
}
/**
 * 删除文件或目录
 * @param filePath:删除的文件名或目录名
 */
public static void delete(String filePath)throws IOException {
	Configuration conf = new Configuration();
	conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
	System.setProperty("HADOOP_USER_NAME", "root");
	FileSystem fs =FileSystem.get(conf);
	Path path = new Path(filePath);
	boolean isok = fs.deleteOnExit(path);
	if(isok) {
		System.out.println("delete ok!");
	}else {
		System.out.println("delete failure!");
	}
	fs.close();
}
/**
 * 检查文件是否存在
 * @param path:文件名
 */
public static void checkFileExist(Path path) {
	try {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
		FileSystem fs =FileSystem.get(conf);
		DistributedFileSystem hdfs = (DistributedFileSystem)fs;
		Path f = hdfs.getHomeDirectory();
		System.out.println("main path:"+f.toString());
		boolean exist = fs.exists(path);
		System.out.println("Whether exist of this files:"+exist);
	} catch (Exception e) {
		// TODO: handle exception
		e.printStackTrace();
	}
}
/**
 * 上传文件
 * @param src:源文件
 * @param src:目的地文件名
 */
public static void uploadFile(String src,String dst)throws IOException,URISyntaxException {
	Configuration conf = new Configuration();
	conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
	System.setProperty("HADOOP_USER_NAME", "root");
	FileSystem fs =FileSystem.get(conf);
	Path srcPath = new Path(src);
	Path dstPath = new Path(dst);
	fs.copyFromLocalFile(false, srcPath,dstPath);
	fs.close();
}
/**
 * 从HDFS下载文件
 * @param remote:HDFS文件
 * @param local:目的地
 */
public static void download(String remote,String local)throws IOException,URISyntaxException {
	Configuration conf = new Configuration();
	Path path = new Path(remote);
	FileSystem fs = FileSystem.get(new URI("hdfs://192.168.254.128:9000"), conf);
	fs.copyToLocalFile(false,path, new Path(local),true);//false表示不删除源文件，true表示使用本地文件系统
	System.out.println("download:from"+remote+" to "+local);
	fs.close();
}
/**
 * 文件重命名
 * @param oldname:旧文件名
 * @param newname:新文件名
 */
public static void rename(String oldname,String newname)throws IOException {
	Configuration conf = new Configuration();
	conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
	System.setProperty("HADOOP_USER_NAME", "root");
	FileSystem fs = FileSystem.get(conf);
	Path oldPath = new Path(oldname);
	Path newpPath = new Path(newname);
	boolean isok = fs.rename(oldPath, newpPath);
	if (isok) {
		System.out.println("rename ok!");
	} else {
		System.out.println("rename failure!");
	}
	fs.close();
}
/**
 * 遍历文件和目录
 * @param path:文件路径
 */
public static void showDir(Path path)throws Exception {
	Configuration conf = new Configuration();
	conf.set("fs.defaultFS", "hdfs://192.168.254.128:9000");
	System.setProperty("HADOOP_USER_NAME", "root");
	FileSystem fs = FileSystem.get(conf);
	DistributedFileSystem hdfs = (DistributedFileSystem)fs;
	FileStatus[] fileStatus = hdfs.listStatus(path);
	if (fileStatus.length > 0) {
		for(FileStatus status:fileStatus) {
			Path f = status.getPath();
			System.out.println(f);
			//判断是否为目录，是的话循环遍历嵌套访问
			if(status.isDirectory()) {
				FileStatus[] files = hdfs.listStatus(f);
				if (files.length > 0) {
					for(FileStatus file:files)
						showDir(file.getPath());
				}
			}
		}
	}
}
/**
 * 取得数据块所在的位置
 * @param path:文件路径
 */
public static void getLocation(Path path) {
	try {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.128:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
		FileSystem fs = FileSystem.get(conf);
		FileStatus fileStatus = fs.getFileStatus(path);
		BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		for(BlockLocation currentLocation:blkLocations) {
			String[] hosts = currentLocation.getHosts();
			for(String host:hosts) {
				System.out.println(host);
			}
		}
		//取得最后修改时间
		long modifyTime = fileStatus.getModificationTime();
		Date d =new Date(modifyTime);
		System.out.println(d);
	} catch (Exception e) {
		// TODO: handle exception
		e.printStackTrace();
	}
}
public static void main(String[] args) throws IllegalArgumentException,Exception{
	//listDataNodeInfo();
	//mkdir("/lib/mysql");
	delete("/data//data1/output2");
	//checkFileExist(new Path("/test"));
	//uploadFile("C:\\Users\\walker\\Downloads\\mysql-connector-java-5.1.47\\mysql-connector-java-5.1.47-bin.jar", "/lib/mysql");
	//download("/test/GH.docx", "V:\\");
	//rename("/test/GH.docx", "/test/GHSB.docx");
	//showDir(new Path("/"));
	//getLocation(new Path("/test/GHSB.docx"));
}
}

