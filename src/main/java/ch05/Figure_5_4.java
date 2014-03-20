package ch05;

import graph.FindDistanceMapper;
import graph.FindDistanceReducer;
import graph.ProduceAdjacencyMatrixCombiner;
import graph.ProduceAdjacencyMatrixMapper;
import graph.ProduceAdjacencyMatrixReducer;

import misc.Nodes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
		Job job = Job.getInstance(conf, "Create Friend Adjacency Matrix from Circles");
		job.setJarByClass(Figure_5_4.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ProduceAdjacencyMatrixMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MapWritable.class);
		job.setCombinerClass(ProduceAdjacencyMatrixCombiner.class);
		job.setReducerClass(ProduceAdjacencyMatrixReducer.class);

		// this should be linear to the number of tasktrackers in the cluster to
		// ensure the output files (which are input to the next iteration) are
		// numerous enough to take advantage of the full cluster on subsequent
		// iterations
		// NOTE: if the input files are large enough, this will happen anyway
		// when the output of the phase is chunked by the HDFS block size; there
		// is still performance advantage to spreading reduction across the
		// cluster
		//job.setNumReduceTasks(100);

		return job.waitForCompletion(true);
	}

	private static long explodeLevel(int i, Configuration conf, Path input, Path output)
			throws Exception {
		conf.set("iteration", new Integer(i).toString());
		Job job = Job.getInstance(conf, String.format("Breadth-First Search Pass %d", i));
		job.setJarByClass(Figure_5_4.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(FindDistanceMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		// performance could be radically improved with a combiner
		job.setReducerClass(FindDistanceReducer.class);

		// see the comment in produceAdjacencyMatrix()
		//job.setNumReduceTasks(100);

		if (!job.waitForCompletion(true)) {
			return -1L;
		}

		return job.getCounters().findCounter(Nodes.ALTERED).getValue();
	}
}
