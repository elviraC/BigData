package mapreduce.frequency;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

		String[] line = value.toString().split(" ");
		for (String word : line) {
			// System.out.println(word);
			for (int i = 0; i < word.length(); i++) {
				try {
					if (Character.isLetter(word.charAt(i))) {
						String k = word.charAt(i) + "_" + word.charAt(i + 1);
						ctx.write(new Text(k.toUpperCase()), new LongWritable(1));
					}
					// System.out.println(word.charAt(i) + "-" + word.charAt(i +
					// 1));

				} catch (StringIndexOutOfBoundsException e) {
					// end of word
				}
			}
		}

	}

}
