package invertedindex;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import misc.TextPairWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexComplexKeyMapper
		extends Mapper<Text, Text, TextPairWritable, DoubleWritable> {

	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		String docID = key.toString();
		
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
				String word = document.substring(lastWordIndex, wordIndex).toLowerCase();
				if (counts.containsKey(word)) {
					counts.put(word, counts.get(word) + 1);
				} else {
					counts.put(word, 1);
				}
				++totalWords;
			}
		}

		for (String word : counts.keySet()) {
			context.write(
					new TextPairWritable(word, docID),
					new DoubleWritable(1.0d*counts.get(word)/totalWords));
		}
	}
}
