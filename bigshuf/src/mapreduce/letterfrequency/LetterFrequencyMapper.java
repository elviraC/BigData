package mapreduce.letterfrequency;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterFrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
		String[] line = value.toString().split(" ");
		for (String word : line) {
			word = word.toLowerCase();
			for (char c : word.toCharArray()) {
				ctx.write(new Text(c + ""), new LongWritable(1));
			}
		}

	}

}
