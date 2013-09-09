package misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class StringIntMapWritable implements WritableComparable<StringIntMapWritable> {
	private Map<String, Integer> counts;
	
	public StringIntMapWritable() {
		set(new HashMap<String, Integer>());
	}
	
	public StringIntMapWritable(HashMap<String, Integer> counts) {
		set(counts);
	}
	
	public void set(HashMap<String, Integer> counts) {
		this.counts = counts;
	}
	
	public void clear() {
		counts.clear();
	}
	
	public Iterable<String> getCountKeys() {
		return counts.keySet();
	}
	
	public Integer getCount(String key) {
		return counts.get(key);
	}

	public void setCount(String key, Integer value) {
		counts.put(key, value);
	}

	public void addCount(String key, Integer value) {
		if (counts.containsKey(key)) {
			counts.put(key, counts.get(key) + value);
		} else {
			counts.put(key, value);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		MapWritable countsWritable = new MapWritable();
		for (String key : getCountKeys()) {
			countsWritable.put(new Text(key), new IntWritable(getCount(key)));
		}
		countsWritable.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		MapWritable countsWritable = new MapWritable();
		countsWritable.readFields(in);
		counts = new HashMap<String, Integer>();
		for (Writable key : countsWritable.keySet()) {
			counts.put(key.toString(), ((IntWritable)countsWritable.get(key)).get());
		}
	}
	
	@Override
	public int hashCode() {
		return counts.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof StringIntMapWritable) {
			StringIntMapWritable other = (StringIntMapWritable) o;
			for (String key : counts.keySet()) {
				if (!other.counts.containsKey(key) || counts.get(key) != other.counts.get(key)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		List<String> parts = new ArrayList<String>();
		for (String key : counts.keySet()) {
			parts.add(key + "," + counts.get(key));
		}
		return StringUtils.join(parts, ";");
	}
	
	@Override
	public int compareTo(StringIntMapWritable other) {
		return 0;
	}
}
