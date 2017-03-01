package com.nokia.chengdu.training.hdfs.base;

public interface Demo {

	default void init() throws Exception {}
	
	void execute() throws Exception;
	
	default void close() throws Exception {}
	
	static void execute(Demo demo) throws Exception {
		demo.init();
		try {
			demo.execute();
		} finally {
			demo.close();
		}
	}
}
