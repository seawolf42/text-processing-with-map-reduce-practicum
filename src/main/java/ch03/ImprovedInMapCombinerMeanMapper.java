package ch03;

import java.io.IOException;

import misc.DoubleIntPairWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ImprovedInMapCombinerMeanMapper extends Mapper<Object, Text, Text, DoubleIntPairWritable> {
	Text x = new Text("x");
	private double sum;
	private int count;
	
	@Override
	public void setup(Context context) {
		sum = 0.0f;
		count = 0;
	}
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		sum += Double.parseDouble(value.toString());
		++count;
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(x, new DoubleIntPairWritable(sum, count));
	}
}