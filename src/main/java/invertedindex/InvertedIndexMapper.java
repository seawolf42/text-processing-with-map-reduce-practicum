package invertedindex;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import misc.ProbabilitiesWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, ProbabilitiesWritable> {
	private String docID = null;
	
	@Override
	public void setup(Context context) {
		docID = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
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
			ProbabilitiesWritable map = new ProbabilitiesWritable();
			map.setProbability(docID, 1.0d*counts.get(word)/totalWords);
			context.write(new Text(word), map);
		}
	}
}
