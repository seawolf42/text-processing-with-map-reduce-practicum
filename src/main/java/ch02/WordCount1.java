package ch02;

//from figure 2.3, basic (primitive) word count

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Locale;

import misc.SentenceInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount1 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Primitive Word Counts");

		job.setJarByClass(WordCount1.class);

		job.setInputFormatClass(SentenceInputFormat.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	static final IntWritable one = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Locale locale = new Locale("en", "US");
		BreakIterator wordIterator = BreakIterator.getWordInstance(locale);

		String sentence = value.toString();
		wordIterator.setText(sentence);
		int wordIndex;
		int lastWordIndex;
		wordIndex = wordIterator.first();
		while (BreakIterator.DONE != wordIndex) {
			lastWordIndex = wordIndex;
			wordIndex = wordIterator.next();
			if ((BreakIterator.DONE != wordIndex) &&
					Character.isLetterOrDigit(sentence.charAt(lastWordIndex))) {
				String token = sentence.substring(lastWordIndex, wordIndex);
				context.write(new Text(token), one);
			}
		}
	}
}

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}
}
