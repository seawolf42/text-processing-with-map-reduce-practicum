package misc;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Locale;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
 
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
 
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

public class SentenceInputFormat extends FileInputFormat<LongWritable, Text> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
 
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
        return new SentenceRecordReader();
    }
}

class SentenceRecordReader extends RecordReader<LongWritable, Text> {

    private FileSplit fileSplit;
    private Configuration conf;
    private boolean processed = false;
    String document;
  
    private LongWritable key = new LongWritable(0);
    private Text value = new Text();
    
    private BreakIterator sentenceIterator;
    private int index = 0;
    private int length = 0;
 
    @Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.conf = taskAttemptContext.getConfiguration();
		Locale locale = new Locale("en", "US");
		sentenceIterator = BreakIterator.getSentenceInstance(locale);
    }
 
    @Override
	public boolean nextKeyValue() throws IOException {
        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
 
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
 
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);                
                document = new String(contents);
                length = document.length();
            } finally {
                IOUtils.closeStream(in);
            }
            sentenceIterator.setText(document);
    		index = sentenceIterator.first();
            processed = true;
    		if (index == BreakIterator.DONE) {
    			return false;
    		}
        }
		int lastIndex = index;
		index = sentenceIterator.next();
		if (index == BreakIterator.DONE) {
			return false;
		}
		key = new LongWritable(index);
		if (Character.isLetterOrDigit(document.charAt(lastIndex))) {
			value = new Text(document.substring(lastIndex, index).toLowerCase());
        } else {
        	value = new Text("ERROR");
        }
		return true;
    }
 
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }
 
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException  {
        return processed ? 1.0f * index / length : 0.0f;
    }
 
    @Override
    public void close() throws IOException {
        // do nothing
    }
}
