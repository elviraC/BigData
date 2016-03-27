package mapreduce.bigshuf.diff;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DiffMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
		
		String [] line  = value.toString().split(" ");
		for(String word: line){
			long ammount = (long) (0.0 + ammountOfDifferentLetters(word));
			ctx.write(new Text("word"), new LongWritable(ammount));
		}
		
	}
	
	public Double ammountOfDifferentLetters(String word) {
		ArrayList<Character> differentLetter = new ArrayList<>();

		for (int i = 0; i < word.length(); i++) {
			char current = word.charAt(i);
			if (!differentLetter.contains(current)) {
				differentLetter.add(current);
			}
		}

		return (double) differentLetter.size();
	}

}
