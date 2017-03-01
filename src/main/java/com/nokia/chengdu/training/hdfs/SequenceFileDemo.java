package com.nokia.chengdu.training.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.VIntWritable;

import com.nokia.chengdu.training.hdfs.base.Console;
import com.nokia.chengdu.training.hdfs.base.Demo;
import com.nokia.chengdu.training.hdfs.base.HdfsClient;
import com.nokia.chengdu.training.hdfs.base.HdfsDemo;
import com.nokia.chengdu.training.hdfs.type.FullName;

public class SequenceFileDemo extends HdfsDemo {

	private static final int ENTRY_COUNT = 100_000;
	
	public SequenceFileDemo() {
		super(
				"Create sequence files",
				"Parse sequence files"
		);
	}
	
	@Override
	protected Demo execute(int menuItemIndex) throws Exception {
		switch (menuItemIndex) {
		case 0:
			this.createSequenceFiles();
			break;
		case 1:
			this.parseSequenceFiles();
			break;
		}
		return null;
	}

	private void createSequenceFiles() throws IOException {
		Path dirPath = this.readPath("Please enter a directory to store three sequence files", ExistenceType.NOT_FILE);
		if (!this.hdfs.exists(dirPath)) {
			this.hdfs.mkdirs(dirPath);
		}
		this.createSequenceFile(new Path(dirPath, "no-compression.seq"), SequenceFile.CompressionType.NONE);
		this.createSequenceFile(new Path(dirPath, "compress-record.seq"), SequenceFile.CompressionType.RECORD);
		this.createSequenceFile(new Path(dirPath, "compress-block.seq"), SequenceFile.CompressionType.BLOCK);
	}
	
	private void parseSequenceFiles() throws FileNotFoundException, IOException {
		Path dirPath = this.readPath("Please enter a directory of sequence files", ExistenceType.DIRECTORY);
		RemoteIterator<LocatedFileStatus> itr = this.hdfs.listFiles(dirPath, false);
		while (itr.hasNext()) {
			LocatedFileStatus lfs = itr.next();
			this.parseSequenceFile(lfs.getPath());
		}
	}
	
	private void createSequenceFile(Path path, SequenceFile.CompressionType compressionType) throws IOException {
		
		/*
		 * Declare the key and value which are reused by the loop statement again and again. 
		 * don't create new data object in loop statement, this is very important for big data technologies.
		 */
		VIntWritable key = new VIntWritable();
		FullName value = new FullName();
		
		try (FSDataOutputStream outputStream = this.hdfs.create(path, true);
				SequenceFile.Writer writer = HdfsClient.sequenceFileWriter(
					SequenceFile.Writer.stream(outputStream),
					SequenceFile.Writer.keyClass(VIntWritable.class),
					SequenceFile.Writer.valueClass(FullName.class),
					SequenceFile.Writer.compression(compressionType)
			)
		) {
			for (int i = ENTRY_COUNT; i > 0; --i) {
				key.set(i);
				value.getFirst().set("first name of " + i); // Faster than value.setFirst(new Text("first name of" + i));
				value.getLast().set("last name of " + i); // Faster than value.setLast(new Text("last name of" + i));
				writer.append(key, value);
			}
		}
		
		Console.writeLine("Sequence file \"%s\" is created, its length is %d", path, this.hdfs.getContentSummary(path).getLength());
	}
	
	private void parseSequenceFile(Path path) throws IOException {
		
		/*
		 * Declare the key and value which are reused by the loop statement again and again. 
		 * don't create new data object in loop statement, this is very important for big data technologies.
		 */
		VIntWritable key = new VIntWritable();
		FullName value = new FullName();
		
		int entryCount = 0;
		int syncPointCount = 0;
		try (SequenceFile.Reader reader = HdfsClient.sequenceFileReader(SequenceFile.Reader.file(path))) {
			while (reader.next(key, value)) {
				entryCount++;
				if (reader.syncSeen()) {
					syncPointCount++;
				}
			}
		}
		
		Console.writeLine(
				"Sequence file \"%s\" is parsed, its entry count is %d, and its sync point count is %d", 
				path, 
				entryCount,
				syncPointCount
		);
	}
}
