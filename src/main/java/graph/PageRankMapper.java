package graph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] keyvalue = value.toString().split("\t");
		Text pageID = new Text(keyvalue[0]);

		context.write(pageID, new Text(keyvalue[1]));
		String[] links = keyvalue[1].split(" ");
		Double pageRank = Double.parseDouble(links[0]) / (links.length-1);
		Text rankMass = new Text(pageRank.toString());
		for (int i = 1; i < links.length; ++i) {
			context.write(new Text(links[i]), rankMass);
		}
	}
}
