package wordcount;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountImprovedInMapCombiningMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Map<String, Integer> wordCounts = new HashMap<String, Integer>();
	
	@Override
	public void setup(Context context) {
		wordCounts.clear();
	}
	
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
				String word = sentence.substring(lastWordIndex, wordIndex);
				if (!wordCounts.containsKey(word)) {
					wordCounts.put(word, 0);
				}
				wordCounts.put(word, wordCounts.get(word) + 1);
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (String word : wordCounts.keySet()) {
			context.write(new Text(word), new IntWritable(wordCounts.get(word)));
		}
	}
}
