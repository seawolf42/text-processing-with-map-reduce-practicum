package ch05;

import graph.PageRankMapper;
import graph.PageRankReducer;
import misc.Nodes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Figure_5_8 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path input = new Path(args[0]);
		String outputBase = args[1];
		Path output = new Path(outputBase);

		long numChanged = Long.MAX_VALUE;
		int i = 1;
		while (numChanged > 0) {
			output = new Path(String.format("%s-%d", outputBase, i));

			numChanged = iterate(i, conf, input, output);
			if (numChanged < 0) {
				System.out.println(String.format("error in iteration %s", i));
				System.exit(2);
			}

			input = output;
			++i;
		}

		System.exit(0);
	}

	private static long iterate(int i, Configuration conf, Path input, Path output)
			throws Exception {
		conf.set("iteration", new Integer(i).toString());
		Job job = Job.getInstance(conf, String.format("PageRank Pass %d", i));
		job.setJarByClass(Figure_5_8.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(PageRankMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// performance could be radically improved with a combiner
		job.setReducerClass(PageRankReducer.class);

		// see the comment in produceAdjacencyMatrix() in Figure_5_4.java
		//job.setNumReduceTasks(100);

		if (!job.waitForCompletion(true)) {
			return -1L;
		}

		return job.getCounters().findCounter(Nodes.ALTERED).getValue();
	}
}
