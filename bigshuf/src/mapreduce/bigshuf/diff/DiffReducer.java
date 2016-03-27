package mapreduce.bigshuf.diff;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> values, Context ctx) throws IOException, InterruptedException {
		int ammountOfDivisors = 0;
		Double sum = 0.0;
		for (LongWritable l : values) {
			ammountOfDivisors++;
			sum += l.get();

		}

		ctx.write(new Text("AVG"), new LongWritable((long) (sum / ammountOfDivisors)));

	}

}
