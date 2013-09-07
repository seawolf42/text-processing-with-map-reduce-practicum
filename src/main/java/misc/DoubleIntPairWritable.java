package misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleIntPairWritable implements WritableComparable<DoubleIntPairWritable> {
	private DoubleWritable sum;
	private IntWritable count;
	
	public DoubleIntPairWritable() {
		set(new DoubleWritable(0.0f), new IntWritable(0));
	}
	
	public DoubleIntPairWritable(double sum, int count) {
		set(new DoubleWritable(sum), new IntWritable(count));
	}
	
	public DoubleIntPairWritable(DoubleWritable sum, IntWritable count) {
		set(sum, count);
	}
	
	public void set(DoubleWritable sum, IntWritable count) {
		this.sum = sum;
		this.count = count;
	}
	
	public DoubleWritable getSum() {
		return sum;
	}
	
	public IntWritable getCount() {
		return count;
	}
	
	public void add(DoubleIntPairWritable other) {
		sum.set(sum.get() + other.getSum().get());
		count.set(count.get() + other.getCount().get());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		sum.write(out);
		count.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		sum.readFields(in);
		count.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return sum.hashCode() * 163 + count.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof DoubleIntPairWritable) {
			DoubleIntPairWritable other = (DoubleIntPairWritable) o;
			return sum.equals(other.sum) && count.equals(other.count);
		}
		return false;
	}
	
	@Override
	public String toString() {
		return sum + "," + count;
	}
	
	@Override
	public int compareTo(DoubleIntPairWritable other) {
		int cmp = sum.compareTo(other.sum);
		if (cmp != 0) {
			return cmp;
		}
		return count.compareTo(other.count);
	}
}
