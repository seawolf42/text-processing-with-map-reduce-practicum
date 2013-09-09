package ch03;

import java.io.IOException;

import misc.StringIntMapWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesCoOccurrenceReducer extends Reducer<Text, StringIntMapWritable, Text, StringIntMapWritable> {
	@Override
	public void reduce(Text key, Iterable<StringIntMapWritable> values, Context context)
			throws IOException, InterruptedException {
		StringIntMapWritable counts = new StringIntMapWritable();
		for (StringIntMapWritable value : values) {
			for (String innerKey : value.getCountKeys()) {
				counts.addCount(innerKey, value.getCount(innerKey));
			}
		}
		context.write(key, counts);
	}
}
