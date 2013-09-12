package ch03;

import meancalculation.ImprovedInMapCombinerMeanMapper;
import meancalculation.ImprovedMeanCombiner;
import meancalculation.ImprovedMeanReducer;
import misc.DoubleIntPairWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Figure_3_7 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Combining Averager with In-Map Combining");

		job.setJarByClass(Figure_3_6.class);

		job.setMapperClass(ImprovedInMapCombinerMeanMapper.class);
		job.setCombinerClass(ImprovedMeanCombiner.class);
		job.setReducerClass(ImprovedMeanReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleIntPairWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
