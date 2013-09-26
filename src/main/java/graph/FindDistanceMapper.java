package graph;

import java.io.IOException;
import java.util.InputMismatchException;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FindDistanceMapper
	extends Mapper<LongWritable, Text, IntWritable, Text> {

	private Integer iteration;
	private Text weight;
	
	@Override
	public void setup(Context context) {
		iteration = context.getConfiguration().getInt("iteration", Integer.MAX_VALUE);
		weight = new Text(iteration.toString());
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] keyvalue = value.toString().split("\t");
		IntWritable personID = new IntWritable(Integer.parseInt(keyvalue[0]));
		
		context.write(personID, new Text(keyvalue[1]));
		Scanner tokenizer = new Scanner(keyvalue[1]);
		Integer distance = tokenizer.nextInt();
		if (distance != Integer.MAX_VALUE) {
			// explode this node
			while (tokenizer.hasNext()) {
				try {
					context.write(new IntWritable(tokenizer.nextInt()), weight);
				} catch (InputMismatchException e) {
					// ignore non-number values; these are 'circleX' items at the start
					// of each line and we don't care about them
					tokenizer.next();
				}
			}
		}
	}
}
