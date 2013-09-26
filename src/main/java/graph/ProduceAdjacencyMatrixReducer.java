package graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class ProduceAdjacencyMatrixReducer
	extends Reducer<IntWritable, MapWritable, IntWritable, Text> {

	private static Integer infinity = Integer.MAX_VALUE;
	
	@Override
	public void reduce(IntWritable key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {

		int personOfInterest = context.getConfiguration().getInt("startNodeId", Integer.MIN_VALUE);

		TreeSet<Integer> result = new TreeSet<Integer>();
		for (MapWritable set : values) {
			for (Writable innerKey : set.keySet()) {
				result.add(((IntWritable)innerKey).get());
			}
		}

		List<String> friends = new ArrayList<String>();
		if (Integer.parseInt(key.toString()) == personOfInterest) {
			friends.add("0");
		} else {
			friends.add(infinity.toString());
		}
		for (Integer friendID : result) {
			friends.add(friendID.toString());
		}
		context.write(key, new Text(StringUtils.join(friends, " ")));
	}
}
