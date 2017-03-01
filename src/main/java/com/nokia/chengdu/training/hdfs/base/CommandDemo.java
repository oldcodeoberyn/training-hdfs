package com.nokia.chengdu.training.hdfs.base;

import java.util.Vector;

public abstract class CommandDemo implements Demo {

	private String[] menuItems;
	
	protected CommandDemo(String ... menuItems) {
		this.menuItems = menuItems.clone();
	}
	
	@Override
	public final void execute() throws Exception {
		do {
			String[] arr = this.menuItems;
			int len = arr.length;
			for (int i = 0; i < len; i++) {
				Console.write(i + 1);
				Console.write(". ");
				Console.writeLine(arr[i]);
			}
			Console.writeLine("0. Exit");
			int choice = Console.readValue("Please enter your choice", line -> {
				int value = Integer.parseInt(line);
				if (value < 0 || value > len) {
					throw new IllegalArgumentException("The choice must between 0 and " + len);
				}
				return value;
			});
			if (choice == 0) {
				break;
			}
			int menuItemIndex = choice - 1;
			Console.push(this.menuItems[menuItemIndex]);
			try {
				Demo childDemo = this.execute(menuItemIndex);
				if (childDemo != null) {
					Demo.execute(childDemo);
				}
			} finally {
				Console.pop();
			}
		} while (true);
	}

	/**
	 * 
	 * @param menuItemIndex
	 * @return Child demo object or null.
	 */
	protected abstract Demo execute(int menuItemIndex) throws Exception;
	
	protected String getMenuItem(int menuItemIndex) {
		return this.menuItems[menuItemIndex];
	}
}
