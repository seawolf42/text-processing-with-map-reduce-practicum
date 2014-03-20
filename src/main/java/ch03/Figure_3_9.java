package ch03;

import misc.SentenceInputFormat;
import misc.StringIntMapWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import wordcooccurrence.StripesCoOccurrenceMapper;
import wordcooccurrence.StripesCoOccurrenceReducer;

public class Figure_3_9 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Stripes Word Co-Occurrence");

		job.setJarByClass(Figure_3_9.class);

		job.setInputFormatClass(SentenceInputFormat.class);

		job.setMapperClass(StripesCoOccurrenceMapper.class);
		job.setReducerClass(StripesCoOccurrenceReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringIntMapWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
