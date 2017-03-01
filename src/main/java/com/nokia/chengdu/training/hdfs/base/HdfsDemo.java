package com.nokia.chengdu.training.hdfs.base;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class HdfsDemo extends CommandDemo {
	
	private static final Pattern SIMPLEST_PATH_PATTERN = Pattern.compile("(/[^/]+)+");

	protected FileSystem hdfs;
	
	protected HdfsDemo(String ... menuItems) {
		super(menuItems);
	}

	@Override
	public void init() throws IOException {
		this.hdfs = HdfsClient.fileSystem();
	}

	@Override
	public void close() throws IOException {
		FileSystem tmp = this.hdfs;
		if (tmp != null) {
			this.hdfs = null;
			tmp.close();
		}
	}
	
	protected Path readPath(String prompt) {
		return this.readPath(prompt, ExistenceType.NONE, null);
	}
	
	protected Path readPath(String prompt, ExistenceType expectedExistence) {
		return this.readPath(prompt, expectedExistence, null);
	}
	
	protected Path readPath(String prompt, ExistenceType expectedExistence, Consumer<Path> validator) {
		return Console.readValue(prompt, line -> {
			if (!SIMPLEST_PATH_PATTERN.matcher(line).matches()) {
				throw new IllegalArgumentException(
						"The path must match the pattern " + 
						SIMPLEST_PATH_PATTERN.pattern()
				);
			}
			Path path = new Path(line);
			if (expectedExistence != null) {
				try {
					switch (expectedExistence) {
					case EXISTS:
						if (this.hdfs.exists(path)) {
							throw new IllegalArgumentException("The path must be exists");
						}
						break;
					case FILE:
						if (!this.hdfs.isFile(path)) {
							throw new IllegalArgumentException("The path must be file");
						}
						break;
					case DIRECTORY:
						if (!this.hdfs.isDirectory(path)) {
							throw new IllegalArgumentException("The path must be directory");
						}
						break;
					case NOT_EXISTS:
						if (this.hdfs.exists(path)) {
							throw new IllegalArgumentException("The path cannot be exists");
						}
						break;
					case NOT_FILE:
						if (this.hdfs.isFile(path)) {
							throw new IllegalArgumentException("The path cannot be file");
						}
						break;
					case NOT_DIRECTORY:
						if (this.hdfs.isDirectory(path)) {
							throw new IllegalArgumentException("The path cannot be directory");
						}
						break;
					default:
						break;
					}
				} catch (IOException ex) {
					// Do nothing
				}
			}
			if (validator != null) {
				validator.accept(path);
			}
			return path;
		});
	}
	
	protected static enum ExistenceType {
		NONE,
		EXISTS,
		FILE, // exists && isFile
		DIRECTORY, // exists && isDirectory
		NOT_EXISTS,
		NOT_FILE, // !exists || isDirectory
		NOT_DIRECTORY // !exists || isFile
	}
}
