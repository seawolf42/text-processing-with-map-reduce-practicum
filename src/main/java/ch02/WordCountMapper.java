package ch02;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Locale;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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
