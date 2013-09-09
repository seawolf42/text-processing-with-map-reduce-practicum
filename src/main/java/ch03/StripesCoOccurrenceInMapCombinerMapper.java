package ch03;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import misc.StringIntMapWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesCoOccurrenceInMapCombinerMapper extends Mapper<Object, Text, Text, StringIntMapWritable> {
	Map<String, StringIntMapWritable> maps = new HashMap<String, StringIntMapWritable>();
	
	@Override
	public void setup(Context context) {
		maps.clear();
	}
	
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
			String word = words.get(i);
			if (!maps.containsKey(word)) {
				maps.put(word, new StringIntMapWritable());
			}
			StringIntMapWritable counts = maps.get(word);
			for (int j = 0 ; j < words.size() ; ++j) {
				if (i != j) {
					counts.addCount(words.get(j), 1);
				}
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (String word : maps.keySet()) {
			context.write(new Text(word), maps.get(word));
		}
	}
}
