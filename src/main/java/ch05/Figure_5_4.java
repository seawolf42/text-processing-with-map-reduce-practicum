package ch05;

import graph.FindDistanceMapper;
import graph.FindDistanceReducer;
import graph.ProduceAdjacencyMatrixCombiner;
import graph.ProduceAdjacencyMatrixMapper;
import graph.ProduceAdjacencyMatrixReducer;

import misc.Nodes;
import misc.WholeFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Figure_5_4 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path input = new Path(args[0]);
		String outputBase = args[1];
		Path output = new Path(outputBase);

		int startNodeId = Integer.parseInt(args[2]);
		conf.set("startNodeId", new Integer(startNodeId).toString());

		if (!produceAdjacencyMatrix(conf, input, output)) {
			System.out.println("error producing adjacency matrix");
			System.exit(1);
		}

		long numChanged = Long.MAX_VALUE;
		int i = 1;
		while (numChanged > 0) {
			input = output;
			output = new Path(String.format("%s-%d", outputBase, i));

			numChanged = explodeLevel(i, conf, input, output);
			if (numChanged < 0) {
				System.out.println(String.format("error in iteration %s", i));
				System.exit(2);
			}

			++i;
		}

		System.exit(0);
	}

	private static boolean produceAdjacencyMatrix(
			Configuration conf, Path input, Path output)
			throws Exception {
		Job job = new Job(conf, "Create Friend Adjacency Matrix from Circles");
		job.setJarByClass(Figure_5_4.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapperClass(ProduceAdjacencyMatrixMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MapWritable.class);
		job.setCombinerClass(ProduceAdjacencyMatrixCombiner.class);
		job.setReducerClass(ProduceAdjacencyMatrixReducer.class);

		return job.waitForCompletion(true);
	}

	private static long explodeLevel(int i, Configuration conf, Path input, Path output)
			throws Exception {
		conf.set("iteration", new Integer(i).toString());
		Job job = new Job(conf, String.format("Breadth-First Search Pass %d", i));
		job.setJarByClass(Figure_5_4.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(FindDistanceMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(FindDistanceReducer.class);

		if (!job.waitForCompletion(true)) {
			return -1L;
		}

		return job.getCounters().findCounter(Nodes.ALTERED).getValue();
	}
}
