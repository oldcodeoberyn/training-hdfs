package com.nokia.chengdu.training.hdfs.base;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class HdfsClient {

	private static final String HADOOP_USER_NAME = "root";
	
	private static final Configuration CONF;
	
	static {
		Configuration conf = new Configuration();
		
		/*
		 * In actual project, 
		 * you should copy the "core-site.xml" and "hdfs-site.xml" from any hadoop server,
		 * then add both of them into the configuration object.
		 * 
		 * In order to let you know what properties are necessary for client side, 
		 * I don't do it but only copy slightest properties into the "hdfs-client.xml".
		 * 
		 * (1) "FileSystem.get(Configuration)" returns an instance of 
		 * "org.apache.hadoop.hdfs.DistributedFileSystem"
		 * because of this configuration.
		 * 
		 * (2) If you don't add any resource into the configuration object, 
		 * "FileSystem.get(Configuration)" returns an instance of
		 * "org.apache.hadoop.fs.LocalFileSystem"
		 * 
		 * (3) Except "Local File System" and "Hadoop Distributed File System",
		 * there is another important distributed file system: Amazon S3 File System.
		 * Please view "https://wiki.apache.org/hadoop/AmazonS3" to know more.
		 */
		conf.addResource("hdfs-client.xml");
//		conf.addResource("hdfs-site.xml");
//		conf.addResource("core-site.xml");

		CONF = conf;
		
		System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);
	}
	
	public static FileSystem fileSystem() throws IOException {
		return FileSystem.get(CONF);
	}
	
	public static FileContext fileContext() throws UnsupportedFileSystemException {
		return FileContext.getFileContext(CONF);
	}
	
	public static CompressionCodecFactory compressionCodecFactory() {
		return new CompressionCodecFactory(CONF);
	}
	
	public static SequenceFile.Writer sequenceFileWriter(SequenceFile.Writer.Option ... options) throws IOException {
		return SequenceFile.createWriter(CONF, options);
	}
	
	public static SequenceFile.Reader sequenceFileReader(SequenceFile.Reader.Option ... options) throws IOException {
		return new SequenceFile.Reader(CONF, options);
	}
	
	public static MapFile.Writer mapFileWriter(Path mapDirPath, SequenceFile.Writer.Option ... options) throws IOException {
		return new MapFile.Writer(CONF, mapDirPath, options);
	}
	
	public static MapFile.Reader mapFileReader(Path mapDirPath, SequenceFile.Reader.Option ... options) throws IOException {
		return new MapFile.Reader(mapDirPath, CONF, options);
	}
}
