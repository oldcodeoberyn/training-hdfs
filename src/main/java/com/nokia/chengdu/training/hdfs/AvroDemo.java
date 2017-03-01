package com.nokia.chengdu.training.hdfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.nokia.chengdu.training.hdfs.base.Console;
import com.nokia.chengdu.training.hdfs.base.Demo;
import com.nokia.chengdu.training.hdfs.base.HdfsClient;
import com.nokia.chengdu.training.hdfs.base.HdfsDemo;
import com.nokia.chengdu.training.hdfs.type.Department;
import com.nokia.chengdu.training.hdfs.type.Employee;
import com.nokia.chengdu.training.hdfs.type.Gender;

public class AvroDemo extends HdfsDemo {

	public AvroDemo() {
		super(
				"Serialziation",
				"Deserialiation"
		);
	}
	
	@Override
	protected Demo execute(int menuItemIndex) throws Exception {
		switch (menuItemIndex) {
		case 0:
			this.serialization();
			break;
		case 1:
			this.deserialization();
			break;
		}
		return null;
	}
	
	private void serialization() throws IOException {
		/*
		 * Note:
		 * 
		 * Avro classes don't implements the hadoop serialization interface "org.apache.hadoop.io.Writable",
		 * and it cannot support the high performance comparator "org.apache.hadoop.io.RawComparator".
		 * 
		 * Please don't use it to the key type!
		 */
		Path path = this.readPath("Please enter the output avro file path", ExistenceType.NOT_DIRECTORY);
		try (FSDataOutputStream outputStream = this.hdfs.create(path, true);
				DataFileWriter<Department> writer = new DataFileWriter<>(new SpecificDatumWriter<>(Department.class))
		) {
			writer.create(Department.SCHEMA$, outputStream); // Must open avro writer manully
			writer.setSyncInterval(100); // Like Hadoop sequence file, it supports sync points
			
			for (Department department : createSmallData()) {
				writer.append(department);
			}
		}
		
		Console.writeLine("The size of the generated file \"%s\" is %d", path, this.hdfs.getContentSummary(path).getLength());
	}
	
	private void deserialization() throws IOException {
		
		Department department = new Department();
		
		Path path = this.readPath("Please enter the input avro file path", ExistenceType.FILE);
		try (DataFileReader<Department> reader = new DataFileReader<>(
				new AvroFSInput(HdfsClient.fileContext(), path), 
				new SpecificDatumReader<>(Department.class)
		)) {
			while (reader.hasNext()) {
				reader.next(department);
				Console.writeLine(department.toString());
			}
		}
	}
	
	private static List<Department> createSmallData() {
		return Arrays.asList(
				Department
				.newBuilder()
				.setName("Design")
				.setEmployees(
						Arrays.asList(
								Employee
								.newBuilder()
								.setFirstName("William")
								.setLastName("King")
								.setGender(Gender.MALE)
								.build(),
								Employee
								.newBuilder()
								.setFirstName("Amanda")
								.setLastName("Taylor")
								.setGender(Gender.FEMALE)
								.build()
						)
				)
				.build(),
				Department
				.newBuilder()
				.setName("Develop")
				.setEmployees(
						Arrays.asList(
								Employee
								.newBuilder()
								.setFirstName("Marcus")
								.setLastName("Cooper")
								.setGender(Gender.MALE)
								.build(),
								Employee
								.newBuilder()
								.setFirstName("Yilia")
								.setLastName("Collins")
								.setGender(Gender.FEMALE)
								.build()
						)
				)
				.build()
		);
	}
}
