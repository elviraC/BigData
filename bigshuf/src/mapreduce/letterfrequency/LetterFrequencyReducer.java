package mapreduce.letterfrequency;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterFrequencyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> values, Context ctx) throws IOException, InterruptedException {

		int sum = 0;
		for (LongWritable l : values) {
			sum += l.get();
		}
		if (Character.isAlphabetic(key.toString().charAt(0))) {
			ctx.write(key, new LongWritable(sum));
		}

	}

}
