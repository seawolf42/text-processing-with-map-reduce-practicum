package ch03;

import java.io.IOException;

import misc.TextPairWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneToManyReducer extends Reducer<TextPairWritable, Text, IntWritable, Text> {
	String s = null;

	@Override
	public void reduce(TextPairWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {	
		int id = Integer.parseInt(key.getFirst().toString());
		
		if (key.getSecond().toString().equals("S")) {
			// there is only one element, since (id,"S") is a unique row
			s = values.iterator().next().toString();
		} else {
			IntWritable tId = new IntWritable(id);
			
			// for a given id, there may be many (id,"T") elements
			for (Text value : values) {
				context.write(tId, new Text(s + "," + value));
			}
			
			// at this point there will be no more 'T' elements for this 'S' element,
			// so set s to null to represent our expectation that the next row
			// will be an 'S' (we will get an exception if it isn't when we try
			// writing additional lines to context in the iteration over 'T' values
			s = null;
		}
	}
}
