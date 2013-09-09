package misc;

// partition text-text keys based on the left key only

import org.apache.hadoop.mapreduce.Partitioner;

public class TextPairLeftWordPartitioner extends Partitioner<TextPairWritable, Object> {
	@Override
	public int getPartition(TextPairWritable key, Object value, int numPartitions) {
		return key.hashCodeFirst() % numPartitions;
	}
}
