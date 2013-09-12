package ch03;

// same as Figure_3_9_v2 except compute frequencies as discussed on p57

import misc.SentenceInputFormat;
import misc.StringIntMapWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import wordcooccurrence.StripesCoOccurrenceInMapCombinerMapper;
import wordcooccurrence.StripesCoOccurrenceWithMarginalReducer;

public class Figure_3_9v3 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Stripes Word Co-Occurrence with In-Map Combiner");

		job.setJarByClass(Figure_3_9v3.class);

		job.setInputFormatClass(SentenceInputFormat.class);

		job.setMapperClass(StripesCoOccurrenceInMapCombinerMapper.class);
		job.setReducerClass(StripesCoOccurrenceWithMarginalReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringIntMapWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
