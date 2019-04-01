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
	 * �г�����Datanode������
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
 * �����ļ�Ŀ¼
 * @param Path:�������ļ���
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
 * ɾ���ļ���Ŀ¼
 * @param filePath:ɾ�����ļ�����Ŀ¼��
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
 * ����ļ��Ƿ����
 * @param path:�ļ���
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
 * �ϴ��ļ�
 * @param src:Դ�ļ�
 * @param src:Ŀ�ĵ��ļ���
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
 * ��HDFS�����ļ�
 * @param remote:HDFS�ļ�
 * @param local:Ŀ�ĵ�
 */
public static void download(String remote,String local)throws IOException,URISyntaxException {
	Configuration conf = new Configuration();
	Path path = new Path(remote);
	FileSystem fs = FileSystem.get(new URI("hdfs://192.168.254.128:9000"), conf);
	fs.copyToLocalFile(false,path, new Path(local),true);//false��ʾ��ɾ��Դ�ļ���true��ʾʹ�ñ����ļ�ϵͳ
	System.out.println("download:from"+remote+" to "+local);
	fs.close();
}
/**
 * �ļ�������
 * @param oldname:���ļ���
 * @param newname:���ļ���
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
 * �����ļ���Ŀ¼
 * @param path:�ļ�·��
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
			//�ж��Ƿ�ΪĿ¼���ǵĻ�ѭ������Ƕ�׷���
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
 * ȡ�����ݿ����ڵ�λ��
 * @param path:�ļ�·��
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
		//ȡ������޸�ʱ��
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

