package ch03;

// same as Figure_3_8 except compute frequencies as discussed on p58

import misc.SentenceInputFormat;
import misc.TextPairLeftWordPartitioner;
import misc.TextPairWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import wordcooccurrence.PairsCoOccurrenceWithMarginalKeyMapper;
import wordcooccurrence.PairsCoOccurrenceWithMarginalReducer;

public class Figure_3_8v2 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Pairs Word Co-Occurrence with Frequencies");

		job.setJarByClass(Figure_3_8v2.class);

		job.setInputFormatClass(SentenceInputFormat.class);

		job.setMapperClass(PairsCoOccurrenceWithMarginalKeyMapper.class);
		job.setPartitionerClass(TextPairLeftWordPartitioner.class);
		job.setReducerClass(PairsCoOccurrenceWithMarginalReducer.class);

		job.setOutputKeyClass(TextPairWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
