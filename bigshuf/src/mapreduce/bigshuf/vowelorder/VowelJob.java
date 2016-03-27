package mapreduce.bigshuf.vowelorder;

import java.io.File;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VowelJob extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());

		String separator = "=";

		final Configuration conf = job.getConfiguration();
		conf.set("mapred.textoutputformat.separator", separator);
		conf.set("mapreduce.textoutputformat.separator", separator);
		conf.set("mapreduce.output.textoutputformat.separator", separator);
		conf.set("mapreduce.output.key.field.separator", separator);
		conf.set("mapred.textoutputformat.separatorText", separator);

		FileInputFormat.addInputPath(job, new Path("/home/mark/Desktop/Big_data/dictionary"));
		FileOutputFormat.setOutputPath(job, new Path("/home/mark/Desktop/Big_data/vowels"));

		job.setMapperClass(VowelMapper.class);
		job.setCombinerClass(VowelReducer.class);
		job.setReducerClass(VowelReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		Date start = new Date();

		if (job.waitForCompletion(true)) {
			Date end = new Date();
			long elapsed = end.getTime() - start.getTime();
			Logger l = Logger.getLogger(VowelJob.class.getName());
			l.log(Level.INFO, "Total time for mapreduce job -> " + elapsed);

			return 0;
		} else {
			return 1;
		}

	}

	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File("//home/mark//Desktop//Big_data/vowels"));
		VowelJob j = new VowelJob();
		int rc = ToolRunner.run(j, args);
		System.exit(rc);

	}

}
