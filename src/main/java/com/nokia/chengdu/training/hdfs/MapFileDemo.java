package com.nokia.chengdu.training.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.nokia.chengdu.training.hdfs.base.Console;
import com.nokia.chengdu.training.hdfs.base.Demo;
import com.nokia.chengdu.training.hdfs.base.HdfsClient;
import com.nokia.chengdu.training.hdfs.base.HdfsDemo;
import com.nokia.chengdu.training.hdfs.type.FullName;

public class MapFileDemo extends HdfsDemo {

	private static final int ENTRY_COUNT = 100_000;
	
	private static final String DIGIT_TEXT_TEMPLATE = "00000000";
	
	private Random random;
	
	public MapFileDemo() {
		super(
				"Create map files",
				"Parse map files"
		);
	}
	
	@Override
	public void init() throws IOException {
		super.init();
		this.random = new Random(System.currentTimeMillis());
	}

	@Override
	protected Demo execute(int menuItemIndex) throws Exception {
		switch (menuItemIndex) {
		case 0:
			this.createMapFiles();
			break;
		case 1:
			this.parseMapFiles();
			break;
		}
		return null;
	}

	private void createMapFiles() throws IOException {
		Path dirPath = this.readPath("Please enter a directory to store three sequence files", ExistenceType.NOT_FILE);
		if (!this.hdfs.exists(dirPath)) {
			this.hdfs.mkdirs(dirPath);
		} else {
			RemoteIterator<LocatedFileStatus> itr = this.hdfs.listLocatedStatus(dirPath);
			while (itr.hasNext()) {
				LocatedFileStatus lfs = itr.next();
				this.hdfs.delete(lfs.getPath(), true);
			}
		}
		this.createMapFile(new Path(dirPath, "no-compression.map"), SequenceFile.CompressionType.NONE);
		this.createMapFile(new Path(dirPath, "compress-record.map"), SequenceFile.CompressionType.RECORD);
		this.createMapFile(new Path(dirPath, "compress-block.map"), SequenceFile.CompressionType.BLOCK);
	}
	
	private void parseMapFiles() throws FileNotFoundException, IOException {
		Path dirPath = this.readPath("Please enter a directory to store three sequence files", ExistenceType.DIRECTORY);
		RemoteIterator<LocatedFileStatus> itr = this.hdfs.listLocatedStatus(dirPath);
		while (itr.hasNext()) {
			Path path = itr.next().getPath();
			if (path.getName().endsWith(".map")) {
				this.parseMapFile(path);
			}
		}
	}
	
	private void createMapFile(Path mapDirPath, SequenceFile.CompressionType compressionType) throws IOException {
		
		/*
		 * Declare the key and value which are reused by the loop statement again and again. 
		 * don't create new data object in loop statement, this is very important for big data technologies.
		 */
		FullName key = new FullName();
		Text value = new Text();
		
		try (MapFile.Writer writer = HdfsClient.mapFileWriter(
				mapDirPath, 
				SequenceFile.Writer.keyClass(FullName.class),
				SequenceFile.Writer.valueClass(Text.class),
				MapFile.Writer.comparator(new FullName.Comparator()),
				SequenceFile.Writer.compression(compressionType)
			)
		) {
			
			writer.setIndexInterval(500); //Can not controll the index file or the map file whose compression type is "BLOCK"
			
			for (int i = 0; i < ENTRY_COUNT; i++) {
				key.getFirst().set("first_" + digitString(i)); // Faster than key.setFirst(new Text(...))
				key.getLast().set("last_" + digitString(i)); // Faster than key.setLast(new Text(...))
				value.set("value_" + i + ": " + UUID.randomUUID().toString());
				writer.append(key, value);
			}
		}
		
		Path indexPath = new Path(mapDirPath, "index");
		Console.writeLine(
				"The size of index file \"%s\" is %d",
				indexPath,
				this.hdfs.getContentSummary(indexPath).getLength()
		);
	}
	
	private void parseMapFile(Path mapDirPath) throws IOException {
		Console.writeLine("Access values of an invalie key and 3 random valid keys of \"%s\"", mapDirPath);
		Console.push(mapDirPath.toString());
		try {
			try (MapFile.Reader reader = HdfsClient.mapFileReader(mapDirPath)) {
				access(reader, ENTRY_COUNT);
				for (int i = 0; i < 3; i++) {
					int randomDigit = this.random.nextInt(ENTRY_COUNT);
					access(reader, randomDigit);
				}
			}
		} finally {
			Console.pop();
		}
	}
	
	private void access(MapFile.Reader reader, int digit) throws IOException {
		
		/*
		 * This method may be invoked again and again.
		 * If you think it's waste to allocate the key and value object again and again,
		 * please refactor them to the member field of current object.
		 * That's a popular design of big data program(but it's a bad idea in Clean-Code theory).
		 */
		FullName key = new FullName();
		Text value = new Text();
		
		key.getFirst().set("first_" + digitString(digit)); // Faster than key.setFirst(new Text(...))
		key.getLast().set("last_" + digitString(digit)); // Faster than key.setLast(new Text(...))
		if (reader.get(key, value) != null) {
			Console.writeLine("The value of \"%s\" is \"%s\"", key, value);
		} else {
			Console.writeLine("No value for the key \"%s\"", key);
		}
	}
	
	private static String digitString(int id) {
		String text = Integer.toString(id);
		int maxLength = DIGIT_TEXT_TEMPLATE.length();
		int padCount = maxLength - text.length();
		if (padCount < 0) {
			throw new IllegalArgumentException("id is too big");
		}
		if (padCount == 0) {
			return text;
		}
		char[] arr = DIGIT_TEXT_TEMPLATE.toCharArray();
		for (int i = padCount; i < maxLength; i++) {
			arr[i] = text.charAt(i - padCount);
		}
		return new String(arr);
	}
}
