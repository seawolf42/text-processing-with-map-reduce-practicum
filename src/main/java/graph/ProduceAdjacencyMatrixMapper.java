package graph;

import java.io.IOException;
import java.util.InputMismatchException;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProduceAdjacencyMatrixMapper extends Mapper<Text, Text, IntWritable, MapWritable> {
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		String keyPrefix = key.toString();
		keyPrefix = keyPrefix.substring(0, keyPrefix.indexOf('.'));
		IntWritable personID = new IntWritable(Integer.parseInt(keyPrefix));

		NullWritable nullValue = NullWritable.get();
		
		MapWritable personAsFriend = new MapWritable();
		personAsFriend.put(personID, nullValue);
		
		MapWritable friends = new MapWritable();
		Scanner tokenizer = new Scanner(value.toString());
		while (tokenizer.hasNext()) {
			try {
				IntWritable friendID = new IntWritable(tokenizer.nextInt());
				friends.put(friendID, nullValue);
				context.write(friendID, personAsFriend);
			} catch (InputMismatchException e) {
				// ignore non-number values; these are 'circleX' items at the start
				// of each line and we don't care about them
				tokenizer.next();
			}
		}

		context.write(personID, friends);
	}
}
