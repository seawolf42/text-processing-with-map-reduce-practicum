package ch03;

import java.io.IOException;

import misc.DoubleIntPairWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ImprovedMeanReducer extends Reducer<Text, DoubleIntPairWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<DoubleIntPairWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0.0f;
		int count = 0;
		for (DoubleIntPairWritable value : values) {
			sum += value.getSum().get();
			count += value.getCount().get();
		}
		context.write(key, new DoubleWritable(sum/count));
	}
}
