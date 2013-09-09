package ch04;

import java.io.IOException;

import misc.ProbabilitiesWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, ProbabilitiesWritable, Text, ProbabilitiesWritable> {
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
		context.write(key, counts);
	}
}
