package com.nokia.chengdu.training.hdfs.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class FullName implements WritableComparable<FullName> {

	private Text first;
	
	private Text last;

	public FullName() {
		this.first = new Text();
		this.last = new Text();
	}

	public FullName(Text first, Text last) {
		this.setFirst(first);
		this.setLast(last);
	}

	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = Objects.requireNonNull(first, "first cannot be null");
	}

	public Text getLast() {
		return last;
	}

	public void setLast(Text last) {
		this.last = Objects.requireNonNull(last, "last cannot be null");
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.first.write(out);
		this.last.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.first.readFields(in);
		this.last.readFields(in);
	}

	@Override
	public int compareTo(FullName o) {
		int cmp = this.first.compareTo(o.first);
		if (cmp != 0) {
			return cmp;
		}
		return this.last.compareTo(o.last);
	}

	@Override
	public int hashCode() {
		return this.first.hashCode() * 31 + this.last.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(this instanceof FullName)) {
			return false;
		}
		FullName other = (FullName)obj;
		return this.first.equals(other.first) && this.last.equals(other.last);
	}

	@Override
	public String toString() {
		return "{ first: " + this.first + ", last: " + this.last + " }"; 
	}
	
	public static class Comparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		public Comparator() {
			super(FullName.class);
		}
		
		/*
		 * Note !!!!!!!!!!!!!!!!!!!!!!!!!!!!
		 * 
		 * Implement method of "org.apache.hadoop.io.RawComparator".
		 * For big data technologies, this optimization is very important!
		 */
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int firstL1, firstL2;
			try {
				firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
			} catch (IOException ex) {
				throw new IllegalArgumentException(ex);
			}
			int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			if (cmp != 0) {
				return cmp;
			}
			return TEXT_COMPARATOR.compare(
					b1, 
					s1 + firstL1, 
					l1 - firstL1, 
					b2, 
					s2 + firstL2, 
					l2 - firstL2
			);
		}
	}
	
	static {
		WritableComparator.define(FullName.class, new Comparator());
	}
}
