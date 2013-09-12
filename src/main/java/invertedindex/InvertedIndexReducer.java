package invertedindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import misc.ProbabilitiesWritable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, ProbabilitiesWritable, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<ProbabilitiesWritable> values, Context context)
			throws IOException, InterruptedException {
		ProbabilitiesWritable counts = new ProbabilitiesWritable();
		for (ProbabilitiesWritable value : values) {
			for (String innerKey : value.keySet()) {
				double count = value.getProbability(innerKey);
				counts.setProbability(innerKey, count);
			}
		}
		List<String> parts = new ArrayList<String>();
		TreeSet<String> sortedKeys = new TreeSet<String>();
		for (String innerKey : counts.keySet()) {
			sortedKeys.add(innerKey);
		}
		for (String innerKey : sortedKeys) {
			parts.add(innerKey + "," + counts.getProbability(innerKey));
		}
		context.write(key, new Text(StringUtils.join(parts, ";")));
	}
}
