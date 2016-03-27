package mapreduce.letterfrequency;

import java.io.File;
import java.util.Date;

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

public class LetterFrequencyJob extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf());

		String separator = "=";

		final Configuration conf = job.getConfiguration();
		conf.set("mapred.textoutputformat.separator", separator); // Prior to
																	// Hadoop 2
																	// (YARN)
		conf.set("mapreduce.textoutputformat.separator", separator); // Hadoop
																		// v2+
																		// (YARN)
		conf.set("mapreduce.output.textoutputformat.separator", separator);
		conf.set("mapreduce.output.key.field.separator", separator);
		conf.set("mapred.textoutputformat.separatorText", separator); // ?

		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());

		FileInputFormat.addInputPath(job, new Path("/home/mark/Desktop/Big_data/alice"));
		FileOutputFormat.setOutputPath(job, new Path("/home/mark/Desktop/Big_data/wordcount"));

		job.setMapperClass(LetterFrequencyMapper.class);
		job.setCombinerClass(LetterFrequencyReducer.class);
		job.setReducerClass(LetterFrequencyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		Date start = new Date();
		if (job.waitForCompletion(true) == true) {
			Date end = new Date();
			long elapsed = (end.getTime() - start.getTime());
			System.out.println(elapsed);

			return 0;
		} else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File("/home/mark/Desktop/Big_data/wordcount"));
		LetterFrequencyJob j = new LetterFrequencyJob();
		int rc = ToolRunner.run(j, args);
		System.exit(rc);

	}

}
