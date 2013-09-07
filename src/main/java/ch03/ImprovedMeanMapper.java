package ch03;

import java.io.IOException;

import misc.DoubleIntPairWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ImprovedMeanMapper extends Mapper<Object, Text, Text, DoubleIntPairWritable> {
	Text x = new Text("x");
	IntWritable one = new IntWritable(1);
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(x,
				new DoubleIntPairWritable(
						new DoubleWritable(Double.parseDouble(value.toString())),
						one)
				);
	}
}
