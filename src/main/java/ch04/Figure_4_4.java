package ch04;

import invertedindex.InvertedIndexComplexKeyMapper;
import invertedindex.InvertedIndexComplexKeyReducer;
import misc.TextPairLeftWordPartitioner;
import misc.TextPairWritable;
import misc.WholeFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Figure_4_4 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Inverted Index Creation Using Complex Keys");

		job.setJarByClass(Figure_4_4.class);

		job.setInputFormatClass(WholeFileInputFormat.class);

		job.setMapperClass(InvertedIndexComplexKeyMapper.class);
		job.setPartitionerClass(TextPairLeftWordPartitioner.class);
		job.setReducerClass(InvertedIndexComplexKeyReducer.class);

		job.setOutputKeyClass(TextPairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
