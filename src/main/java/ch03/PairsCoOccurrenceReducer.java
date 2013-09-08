package ch03;

import java.io.IOException;

import misc.TextPairWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairsCoOccurrenceReducer extends Reducer<TextPairWritable, IntWritable, TextPairWritable, IntWritable> {
	@Override
	public void reduce(TextPairWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}
}
