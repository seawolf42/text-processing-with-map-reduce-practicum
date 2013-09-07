package ch03;

import java.io.IOException;

import misc.DoubleIntPairWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ImprovedMeanCombiner extends Reducer<Text, DoubleIntPairWritable, Text, DoubleIntPairWritable> {
	@Override
	public void reduce(Text key, Iterable<DoubleIntPairWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0.0f;
		int count = 0;
		for (DoubleIntPairWritable value : values) {
			sum += value.getSum().get();
			count += value.getCount().get();
		}
		context.write(key, new DoubleIntPairWritable(sum, count));
	}
}
