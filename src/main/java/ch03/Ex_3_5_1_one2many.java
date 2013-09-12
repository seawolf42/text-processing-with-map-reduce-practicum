package ch03;

import join.OneToManyMapper;
import join.OneToManyReducer;
import misc.TextPairLeftWordPartitioner;
import misc.TextPairWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ex_3_5_1_one2many {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "One-to-Many Join Between Tables S and T");

		job.setJarByClass(Ex_3_5_1_one2many.class);

		job.setMapperClass(OneToManyMapper.class);
		job.setReducerClass(OneToManyReducer.class);
		job.setPartitionerClass(TextPairLeftWordPartitioner.class);

		job.setOutputKeyClass(TextPairWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
