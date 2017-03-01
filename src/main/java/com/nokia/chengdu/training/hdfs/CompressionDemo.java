package com.nokia.chengdu.training.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import com.nokia.chengdu.training.hdfs.base.Console;
import com.nokia.chengdu.training.hdfs.base.Demo;
import com.nokia.chengdu.training.hdfs.base.HdfsClient;
import com.nokia.chengdu.training.hdfs.base.HdfsDemo;

public class CompressionDemo extends HdfsDemo {
	
	private static final String GZ_SUFFIX = ".gz";
	
	private CompressionCodec codec;
	
	private Compressor compressor;
	
	private Decompressor decompressor;

	public CompressionDemo() {
		super(
				"Compress",
				"Decomparess"
		);
	}
	
	@Override
	public void init() throws IOException {
		super.init();
		
		this.codec = HdfsClient.compressionCodecFactory().getCodecByName("gzip");
		this.compressor = CodecPool.getCompressor(this.codec);
		this.decompressor = CodecPool.getDecompressor(this.codec);
	}
	
	@Override
	public void close() throws IOException {
		try {
			CodecPool.returnCompressor(this.compressor);
			CodecPool.returnDecompressor(this.decompressor);
		} finally {
			super.close();
		}
	}

	@Override
	protected Demo execute(int menuItemIndex) throws Exception {
		switch (menuItemIndex) {
		case 0:
			this.compress();
			break;
		case 1:
			this.decomparess();
			break;
		}
		return null;
	}
	
	private void compress() throws IOException {
		Path inputPath = this.readPath("Please enter the path of input file to be compressed", ExistenceType.FILE, path -> {
			if (path.toString().endsWith(GZ_SUFFIX)) {
				throw new IllegalArgumentException("The path cannot end with \"" + GZ_SUFFIX + "\"");
			}
		});
		Path outputPath = this.readPath("Please enter the path of output compressed file(end with \"" + GZ_SUFFIX + "\")", ExistenceType.NOT_DIRECTORY, path -> {
			if (!path.toString().endsWith(GZ_SUFFIX)) {
				throw new IllegalArgumentException("The path must end with \"" + GZ_SUFFIX + "\"");
			}
		});
		try (InputStream inputStream = this.hdfs.open(inputPath);
				OutputStream outputStream = this.codec.createOutputStream(this.hdfs.create(outputPath, true), this.compressor)) {
			IOUtils.copyLarge(inputStream, outputStream);
		}
		Console.writeLine(
				"Original file size: %d, comparessed file size: %d", 
				this.hdfs.getContentSummary(inputPath).getLength(),
				this.hdfs.getContentSummary(outputPath).getLength()
		);
	}
	
	private void decomparess() throws IOException {
		Path inputPath = this.readPath("Please enter the path of input file to be decompressed(end with \"" + GZ_SUFFIX + "\")", ExistenceType.FILE, path -> {
			if (!path.toString().endsWith(GZ_SUFFIX)) {
				throw new IllegalArgumentException("The path must end with \"" + GZ_SUFFIX + "\"");
			}
		});
		Path outputPath = this.readPath("Please enter the path of output file", ExistenceType.NOT_DIRECTORY, path -> {
			if (path.toString().endsWith(GZ_SUFFIX)) {
				throw new IllegalArgumentException("The path cannot end with \"" + GZ_SUFFIX + "\"");
			}
		});
		try (InputStream inputStream = this.codec.createInputStream(this.hdfs.open(inputPath), this.decompressor);
				OutputStream outputStream = this.hdfs.create(outputPath, true)) {
			IOUtils.copyLarge(inputStream, outputStream);
		}
		Console.writeLine(
				"Original file size: %d, decomparessed file size: %d", 
				this.hdfs.getContentSummary(inputPath).getLength(),
				this.hdfs.getContentSummary(outputPath).getLength()
		);
	}
}
