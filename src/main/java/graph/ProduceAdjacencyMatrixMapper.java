package graph;

import java.io.IOException;
import java.util.InputMismatchException;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class ProduceAdjacencyMatrixMapper
	extends Mapper<LongWritable, Text, IntWritable, MapWritable> {

	private IntWritable personID = null;
	private static NullWritable nullValue = NullWritable.get();
	
	@Override
	public void setup(Context context) {
		String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
		personID = new IntWritable(Integer.parseInt(
				filename.substring(0, filename.indexOf('.'))));	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
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
