package invertedindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import misc.TextPairWritable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexComplexKeyReducer extends Reducer<TextPairWritable, DoubleWritable, Text, Text> {
	String lastTerm;
	List<String> postings;
	
	@Override
	public void setup(Context context) {
		lastTerm = null;
		postings = new ArrayList<String>();
	}
	
	@Override
	public void reduce(TextPairWritable key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		String term = key.getFirst().toString();
		String page = key.getSecond().toString();
		
		if (lastTerm != null && !term.equals(lastTerm)) {
			context.write(new Text(lastTerm), new Text(StringUtils.join(postings, ";")));
			postings.clear();
		}
		
		for (DoubleWritable value : values) {
			postings.add(String.format("%s,%.4f", page, value.get()));
		}
		lastTerm = term;
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text(lastTerm), new Text(StringUtils.join(postings, ";")));
	}
}
