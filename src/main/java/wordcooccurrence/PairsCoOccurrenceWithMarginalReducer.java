package wordcooccurrence;

import java.io.IOException;

import misc.TextPairWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairsCoOccurrenceWithMarginalReducer extends Reducer<TextPairWritable, IntWritable, TextPairWritable, DoubleWritable> {
	int marginal = 0;

	@Override
	public void reduce(TextPairWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		if (key.getSecond().toString().equals(TextPairWritable.ACCUMULATOR)) {
			marginal = count;
		} else {
			context.write(key, new DoubleWritable(1.0f*count/marginal));
		}
	}
}
