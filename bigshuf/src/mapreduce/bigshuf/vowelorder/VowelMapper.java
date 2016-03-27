package mapreduce.bigshuf.vowelorder;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VowelMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private String vowels = "eyuoia";

	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

		String[] line = value.toString().split(" ");
		for (String word : line) {
			try {
				if (!firstIsVowel(word.toLowerCase().charAt(0) + "")
						&& lastIsVowel(word.toLowerCase().charAt(word.length() - 1) + "")) {
					ctx.write(new Text("CV"), new LongWritable(1));
				} else {
					ctx.write(new Text("VC"), new LongWritable(1));
				}
			} catch (StringIndexOutOfBoundsException e) {

			}
		}

	}

	public boolean firstIsVowel(String word) {
		return vowels.contains(word.charAt(0) + "") ? true : false;
	}

	public boolean lastIsVowel(String word) {
		return vowels.contains(word.charAt(word.length() - 1) + "") ? true : false;
	}

}
