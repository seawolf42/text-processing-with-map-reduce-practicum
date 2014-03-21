package invertedindex;

import java.io.IOException;

import misc.TextPairWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexComplexKeyReducer extends Reducer<TextPairWritable, DoubleWritable, Text, Text> {
	private String lastTerm = null;
	private StringBuilder builder = new StringBuilder();
	
	@Override
	public void setup(Context context) {
		lastTerm = null;
		builder.setLength(0);
	}
	
	@Override
	public void reduce(TextPairWritable key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException
	{
		String term = key.getFirst().toString();
		String page = key.getSecond().toString();
		
		if (lastTerm != null && !term.equals(lastTerm)) {
			write(context);
			builder.setLength(0);
		}

		lastTerm = term;
		for (DoubleWritable value : values) {
			builder.append(String.format("%s,%.4f:", page, value.get()));
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		write(context);
	}
	
	private void write(Context context) throws IOException, InterruptedException {
		builder.setLength(builder.length() - 1);
		context.write(new Text(lastTerm), new Text(builder.toString()));
	}
}
