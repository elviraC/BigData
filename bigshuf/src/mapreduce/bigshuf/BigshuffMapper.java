package mapreduce.bigshuf;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BigshuffMapper extends Mapper<LongWritable, Text, Text, Text> {

	Analyser an = null;
	StringBuilder builder = new StringBuilder();

	public BigshuffMapper() throws IOException {
		an = new Analyser();
	}

	private static final String[] ignore = { "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", ".", ",", "\\?", "!",
			";", "\"", "'" };

	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

		String[] items = value.toString().split(" ");

		for (String word : items) {
			for (String ig : ignore) {
				word = word.replace(ig, "");
			}
			builder.append(" " + word + " ");
		}

		if (!value.toString().isEmpty()) {
			if (an.predictSentence(builder.toString().toLowerCase()) < 0.20) {
				ctx.write(new Text("WEIRD SENTENCE"), new Text(value.toString()));
			}
		}

		builder = new StringBuilder();

	}
}