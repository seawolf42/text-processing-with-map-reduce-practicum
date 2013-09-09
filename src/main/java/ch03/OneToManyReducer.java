package ch03;

import java.io.IOException;

import misc.TextPairWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneToManyReducer extends Reducer<TextPairWritable, Text, IntWritable, Text> {
	String s = null;
	int sId = -1;

	@Override
	public void reduce(TextPairWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {	
		int id = Integer.parseInt(key.getFirst().toString());
		
		if (key.getSecond().toString().equals("S")) {
			// there is only one element, since (id,"S") is a unique row in S
			s = values.iterator().next().toString();
			sId = id;
		} else {
			IntWritable tId = new IntWritable(id);
			
			for (Text value : values) {
				context.write(tId, new Text(s + "," + value));
			}
		}
	}
}
