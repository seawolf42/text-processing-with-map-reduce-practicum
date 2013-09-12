package wordcooccurrence;

//'co-occurrence' in this algorithm is defined as two words existing in
//the same sentence

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import misc.StringIntMapWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesCoOccurrenceMapper extends Mapper<Object, Text, Text, StringIntMapWritable> {
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
		
		StringIntMapWritable counts = new StringIntMapWritable();

		for (int i = 0 ; i < words.size() ; ++i) {
			counts.clear();
			for (int j = 0 ; j < words.size() ; ++j) {
				if (i != j) {
					counts.addCount(words.get(j), 1);
				}
			}
			context.write(new Text(words.get(i)), counts);
		}
	}
}
