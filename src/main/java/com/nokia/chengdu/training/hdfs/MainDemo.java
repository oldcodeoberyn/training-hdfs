package com.nokia.chengdu.training.hdfs;

import com.nokia.chengdu.training.hdfs.base.CommandDemo;
import com.nokia.chengdu.training.hdfs.base.Demo;

public class MainDemo extends CommandDemo {

	public MainDemo() {
		super(
				"Simple demo",
				"Compression demo",
				"Sequence file demo",
				"Map file demo",
				"Avro demo"
		);
	}
	
	@Override
	protected Demo execute(int menuItemIndex) {
		switch (menuItemIndex) {
		case 0:
			return new SimpleDemo();
		case 1:
			return new CompressionDemo();
		case 2:
			return new SequenceFileDemo();
		case 3:
			return new MapFileDemo();
		case 4:
			return new AvroDemo();
		default:
			return null;
		}
	}
}
