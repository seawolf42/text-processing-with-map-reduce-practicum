package meancalculation;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeanMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	Text x = new Text("x");
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(x, new DoubleWritable(Double.parseDouble(value.toString())));
	}
}
