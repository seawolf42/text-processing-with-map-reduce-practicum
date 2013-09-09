package ch04;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import misc.ProbabilitiesWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<Text, Text, Text, ProbabilitiesWritable> {
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		String docId = key.toString();
		
		Locale locale = new Locale("en", "US");
		BreakIterator wordIterator = BreakIterator.getWordInstance(locale);
		
		Map<String, Integer> counts = new HashMap<String, Integer>();
		
		int totalWords = 0;
		
		String document = value.toString();
		wordIterator.setText(document);
		int wordIndex;
		int lastWordIndex;
		wordIndex = wordIterator.first();
		while (BreakIterator.DONE != wordIndex) {
			lastWordIndex = wordIndex;
			wordIndex = wordIterator.next();
			if ((BreakIterator.DONE != wordIndex) &&
					Character.isLetterOrDigit(document.charAt(lastWordIndex))) {
				String word = document.substring(lastWordIndex, wordIndex);
				if (counts.containsKey(word)) {
					counts.put(word, counts.get(word) + 1);
				} else {
					counts.put(word, 1);
				}
				++totalWords;
			}
		}

		for (String word : counts.keySet()) {
			ProbabilitiesWritable map = new ProbabilitiesWritable();
			map.setProbability(docId, 1.0d*counts.get(word)/totalWords);
			context.write(new Text(word), map);
		}
	}
}
