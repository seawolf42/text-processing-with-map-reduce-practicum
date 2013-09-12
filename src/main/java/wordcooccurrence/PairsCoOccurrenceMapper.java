package wordcooccurrence;

// 'co-occurrence' in this algorithm is defined as two words existing in
// the same sentence

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import misc.TextPairWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairsCoOccurrenceMapper extends Mapper<Object, Text, TextPairWritable, IntWritable> {
	private IntWritable one = new IntWritable(1);

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Locale locale = new Locale("en", "US");
		BreakIterator wordIterator = BreakIterator.getWordInstance(locale);

		List<String> words = new ArrayList<String>();
		
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
				words.add(sentence.substring(lastWordIndex, wordIndex));
			}
		}
		
		for (int i = 0 ; i < words.size() ; ++i) {
			for (int j = 0 ; j < words.size() ; ++j) {
				if (i != j) {
					context.write(new TextPairWritable(words.get(i), words.get(j)), one);
				}
			}
		}
	}
}
