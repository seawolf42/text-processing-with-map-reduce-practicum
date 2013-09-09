package ch03;

import java.io.IOException;

import misc.TextPairWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OneToManyMapper extends Mapper<Object, Text, TextPairWritable, Text> {
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		String table = "";
		int joinKeyIndex = -1;
		if (fields.length == 2) {
			table = "S";
			joinKeyIndex = 0; 
		} else {
			table = "T";
			joinKeyIndex = 1;
		}
		context.write(new TextPairWritable(fields[joinKeyIndex], table), value);
	}
}
