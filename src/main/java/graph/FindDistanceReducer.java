package graph;

import java.io.IOException;

import misc.Nodes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FindDistanceReducer
	extends Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Integer minPathLength = Integer.MAX_VALUE;
		Integer originalPathLength = null;
		String friendsList = null;
		
		for (Text value : values) {
			String node = value.toString();
			int pathLength;
			int firstSpaceIndex = node.indexOf(' ');
			if (firstSpaceIndex >= 0) {
				pathLength = Integer.parseInt(node.substring(0, firstSpaceIndex));
				originalPathLength = pathLength;
				friendsList = node.substring(firstSpaceIndex+1);
			} else {
				pathLength = Integer.parseInt(node);
			}
			minPathLength = Math.min(minPathLength, pathLength);
		}
		
		context.write(key, new Text(String.format("%d %s", minPathLength, friendsList)));
		
		if (minPathLength < originalPathLength) {
			context.getCounter(Nodes.ALTERED).increment(1);
		}
	}
}
