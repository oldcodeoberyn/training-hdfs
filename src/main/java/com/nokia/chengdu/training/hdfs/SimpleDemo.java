package com.nokia.chengdu.training.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.UUID;

import org.apache.hadoop.fs.Path;

import com.nokia.chengdu.training.hdfs.base.Console;
import com.nokia.chengdu.training.hdfs.base.Demo;
import com.nokia.chengdu.training.hdfs.base.HdfsDemo;

public class SimpleDemo extends HdfsDemo {

	public SimpleDemo() {
		super(
				"Write file",
				"Read file"
		);
	}

	@Override
	protected Demo execute(int menuItemIndex) throws IOException {
		switch (menuItemIndex) {
		case 0:
			writeFile();
			break;
		case 1:
			readFile();
			break;
		}
		return null;
	}
	
	private void writeFile() throws IOException {
		Path path = this.readPath("Please enter the output hdfs file path", ExistenceType.NOT_DIRECTORY);
		int count = Console.readValue("Please enter the count of rows to be generated", Integer::parseInt);
		int lastPercentage = 0;
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(this.hdfs.create(path, true)), 4 * 1024)) {
			for (int i = 0; i < count; i++) {
				writer.write(UUID.randomUUID().toString());
				writer.write('\n');
				int percentage = (i + 1) * 100 / count;
				if (lastPercentage != percentage) {
					Console.write(".");
					lastPercentage = percentage;
				}
			}
			Console.writeLine();
		}
		Console.writeLine("The hdfs file \"%s\" has been generated", path);
	}
	
	private void readFile() throws IOException {
		Path path = this.readPath("Please enter the input hdfs file path", ExistenceType.FILE);
		int count = Console.readValue("Please enter max row count to read", line -> {
			int value = Integer.parseInt(line);
			if (value > 10000) {
				throw new IllegalArgumentException("Cannot be greator than 10000");
			}
			return value;
		});
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(this.hdfs.open(path)))) {
			for (int i = count; i > 0; --i) {
				String line = reader.readLine();
				if (line == null) {
					break;
				}
				Console.writeLine(line);
			}
		}
	}
}
