package ch03;

// output frequencies instead of counts

import java.io.IOException;

import misc.StringIntMapWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesCoOccurrenceWithMarginalReducer extends Reducer<Text, StringIntMapWritable, Text, String> {
	@Override
	public void reduce(Text key, Iterable<StringIntMapWritable> values, Context context)
			throws IOException, InterruptedException {
		StringIntMapWritable counts = new StringIntMapWritable();
		int marginal = 0;
		for (StringIntMapWritable value : values) {
			for (String innerKey : value.getCountKeys()) {
				int count = value.getCount(innerKey);
				marginal += count;
				counts.addCount(innerKey, count);
			}
		}
		context.write(key, counts.toFrequencyString(marginal));
	}
}
