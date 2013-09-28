package graph;

import java.io.IOException;

import misc.Nodes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Double originalPageRank = null;
		String links = null;
		Double rankMass = new Double(0.0);

		for (Text value : values) {
			String node = value.toString();
			int firstSpaceIndex = node.indexOf(' ');
			if (firstSpaceIndex >= 0) {
				originalPageRank = Double.parseDouble(node.substring(0, firstSpaceIndex));
				links = node.substring(firstSpaceIndex+1);
			} else {
				rankMass += Double.parseDouble(node);
			}
		}

		context.write(key, new Text(String.format("%f %s", rankMass, links)));

		double differential = rankMass / originalPageRank;
		System.out.println(key + ": " + originalPageRank + "->" + rankMass + " = " + differential);
		if (0.9 > differential || differential > 1.1) {
			context.getCounter(Nodes.ALTERED).increment(1);
		}
	}
}
