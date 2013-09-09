package misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ProbabilitiesWritable implements WritableComparable<ProbabilitiesWritable> {
	private Map<String, Double> probabilities;
	
	public ProbabilitiesWritable() {
		set(new HashMap<String, Double>());
	}
	
	public ProbabilitiesWritable(HashMap<String, Double> probabilities) {
		set(probabilities);
	}
	
	public void set(HashMap<String, Double> probabilities) {
		this.probabilities = probabilities;
	}
	
	public void clear() {
		probabilities.clear();
	}
	
	public Iterable<String> keySet() {
		return probabilities.keySet();
	}
	
	public Double getProbability(String key) {
		return probabilities.get(key);
	}

	public void setProbability(String key, Double value) {
		probabilities.put(key, value);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		MapWritable countsWritable = new MapWritable();
		for (String key : probabilities.keySet()) {
			countsWritable.put(new Text(key), new DoubleWritable(probabilities.get(key)));
		}
		countsWritable.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		MapWritable countsWritable = new MapWritable();
		countsWritable.readFields(in);
		probabilities = new HashMap<String, Double>();
		for (Writable key : countsWritable.keySet()) {
			probabilities.put(key.toString(), ((DoubleWritable)countsWritable.get(key)).get());
		}
	}
	
	@Override
	public int hashCode() {
		return probabilities.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof StringIntMapWritable) {
			ProbabilitiesWritable other = (ProbabilitiesWritable) o;
			for (String key : probabilities.keySet()) {
				if (!other.probabilities.containsKey(key)
						|| probabilities.get(key) != other.probabilities.get(key)) {
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
		for (String key : new TreeSet<String>(probabilities.keySet())) { 
			parts.add(key + "," + probabilities.get(key));
		}
		return StringUtils.join(parts, ";");
	}
	
	@Override
	public int compareTo(ProbabilitiesWritable other) {
		return 0;
	}
}
