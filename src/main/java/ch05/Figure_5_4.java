package ch05;

import graph.ProduceAdjacencyMatrixCombiner;
import graph.ProduceAdjacencyMatrixMapper;
import graph.ProduceAdjacencyMatrixReducer;

import misc.WholeFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
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
//		conf.set("startNodeId", new Integer(startNodeId).toString());
		
		if (!produceAdjacencyMatrix(conf, input, output)) {
			System.out.println("error producing adjacency matrix");
			System.exit(0);
		}
		
		boolean finished = true;
		int i = 1;
		while (!finished) {
			input = output;
			output = new Path(String.format("{0}-{1}", outputBase, i));
			
			Job job = new Job(conf, String.format("Breadth-First Search Pass {0}", i));
			
			
			finished = true;
		}
		
		System.exit(0);
	}
	
	private static boolean produceAdjacencyMatrix(
			Configuration conf, Path input, Path output)
			throws Exception {
		Job job = new Job(conf, "Create Friend Adjacency Matrix from Circles");

		job.setJarByClass(Figure_5_4.class);

		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapperClass(ProduceAdjacencyMatrixMapper.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MapWritable.class);

		job.setCombinerClass(ProduceAdjacencyMatrixCombiner.class);
		job.setReducerClass(ProduceAdjacencyMatrixReducer.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true);
	}
}
