package misc;

// copied from Hadoop, the Definitive Guide, p104

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPairWritable implements WritableComparable<TextPairWritable> {
	private Text first;
	private Text second;
	
	public TextPairWritable() {
		set(new Text(), new Text());
	}
	
	public TextPairWritable(Text first, Text second) {
		set(first, second);
	}
	
	public TextPairWritable(String first, String second) {
		set(new Text(first), new Text(second));
	}
	
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public Text getFirst() {
		return first;
	}
	
	public Text getSecond() {
		return second;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPairWritable) {
			TextPairWritable other = (TextPairWritable) o;
			return first.equals(other.first) && second.equals(other.second);
		}
		return false;
	}
	
	@Override
	public String toString() {
		return first + "," + second;
	}
	
	@Override
	public int compareTo(TextPairWritable other) {
		int cmp = first.compareTo(other.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(other.second);
	}
}
