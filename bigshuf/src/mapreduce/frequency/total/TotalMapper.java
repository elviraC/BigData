package mapreduce.frequency.total;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private Text tmp = new Text();
	private LongWritable occ = new LongWritable();

	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

		try {
			String[] line = value.toString().split("=");
			for (String s : line) {
				try {
					occ.set(Long.parseLong(s));
				} catch (NumberFormatException e) {
					String val = s.charAt(0) + "";
					tmp.set(val.toUpperCase());
				}
			}

			ctx.write(tmp, occ);
		} catch (Exception e) {

		}
	}
}