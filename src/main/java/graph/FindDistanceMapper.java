package graph;

// to make paths measure shortest weights instead of fewest number of steps:
// * alter the input to include weights with each element in the list
// * change the value emitted inside the while loop to be the distance so
//   far plus the weight of the node being tokenized

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
	private Text pathLength;
	
	@Override
	public void setup(Context context) {
		iteration = context.getConfiguration().getInt("iteration", Integer.MAX_VALUE);
		pathLength = new Text(iteration.toString());
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
					context.write(new IntWritable(tokenizer.nextInt()), pathLength);
				} catch (InputMismatchException e) {
					// ignore non-number values; these are 'circleX' items at the start
					// of each line and we don't care about them
					tokenizer.next();
				}
			}
		}
	}
}
