package com.nokia.chengdu.training.hdfs.base;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Stack;
import java.util.function.Function;

public class Console {

	/*
	 * Special reader that can NOT be closed.
	 */
	private static final BufferedReader KEYBOARD_READER = 
			new BufferedReader(new InputStreamReader(System.in));
	
	private static Stack<String> prefixs = new Stack<>();
	
	private static boolean newLine = true;
	
	private Console() {}
	
	public static void push(String prefix) {
		prefixs.push(prefix);
	}
	
	public static void pop() {
		prefixs.pop();
	}
	
	public static void write(char c) {
		printPrefixsIfNecessary();
		System.out.print(c);
		newLine = c == '\n';
	}
	
	public static void write(int n) {
		write(Integer.toString(n));
	}
	
	public static void write(String message) {
		if (message == null || message.isEmpty()) {
			return;
		}
		int lineBeginIndex = 0;
		while (true) {
			printPrefixsIfNecessary();
			int lineEndIndex = message.indexOf('\n', lineBeginIndex);
			if (lineEndIndex == -1) {
				System.out.append(message, lineBeginIndex, message.length());
				return;
			}
			System.out.append(message, lineBeginIndex, lineEndIndex);
			newLine = true;
			lineBeginIndex = lineEndIndex + 1;
		}
	}
	
	public static void write(String format, Object ... args) {
		write(String.format(format, args));
	}
	
	public static void writeLine() {
		printPrefixsIfNecessary();
		System.out.println();
		newLine = true;
	}
	
	public static void writeLine(String message) {
		write(message);
		writeLine();
	}
	
	public static void writeLine(int n) {
		writeLine(Integer.toString(n));
	}
	
	public static void writeLine(String message, Object ... args) {
		write(message, args);
		writeLine();
	}
	
	private static void printPrefixsIfNecessary() {
		if (newLine) {
			newLine = false;
			boolean addSeparator = false;
			for (String prefix : prefixs) {
				if (addSeparator) {
					System.out.print("::");
				} else {
					addSeparator = true;
				}
				System.out.print(prefix);
			}
			System.out.print(">>> ");
		}
	}
	
	public static String readLine(String prompt) {
		if (prompt != null && !prompt.isEmpty()) {
			write(prompt);
			write(": ");
		}
		try {
			String line = KEYBOARD_READER.readLine();
			newLine = true;
			return line;
		} catch (IOException ex) {
			throw new AssertionError("OMG, Impossible! Command shell reading failed", ex);
		}
	}
	
	public static <T> T readValue(String prompt, Function<String, T> valueParser) {
		while (true) {
			String line = readLine(prompt);
			try {
				return valueParser.apply(line);
			} catch (Exception ex) {
				write("Bad input");
				String message = ex.getMessage();
				if (message != null && !message.isEmpty()) {
					write('(');
					write(message);
					write(')');
				}
				writeLine(", please try again: ");
			}
		}
	}
}
