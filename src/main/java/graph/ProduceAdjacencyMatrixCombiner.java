package graph;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ProduceAdjacencyMatrixCombiner
	extends Reducer<IntWritable, MapWritable, IntWritable, MapWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		MapWritable result = new MapWritable();
		for (MapWritable set : values) {
			result.putAll(set);
		}
		
		context.write(key, result);
	}
}
