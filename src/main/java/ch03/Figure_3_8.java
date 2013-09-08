package ch03;

import misc.SentenceInputFormat;
import misc.TextPairWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Figure_3_8 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Pairs Word Co-Occurrence");

		job.setJarByClass(Figure_3_8.class);

		job.setInputFormatClass(SentenceInputFormat.class);

		job.setMapperClass(PairsCoOccurrenceMapper.class);
		job.setReducerClass(PairsCoOccurrenceReducer.class);

		job.setOutputKeyClass(TextPairWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
