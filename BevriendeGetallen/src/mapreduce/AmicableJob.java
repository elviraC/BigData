package mapreduce;

import java.io.File;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AmicableJob extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());

		FileInputFormat.addInputPath(job, new Path("/home/mark/Desktop/Big_data/inputfiles"));
		FileOutputFormat.setOutputPath(job, new Path("/home/mark/Desktop/Big_data/amicable"));

		job.setMapperClass(AmicableMapper.class);

		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		job.setReducerClass(AmicableReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Date start = new Date();

		if (job.waitForCompletion(true)) {
			Date end = new Date();
			long elapsed = end.getTime() - start.getTime();
			Logger l = Logger.getLogger(AmicableJob.class.getName());
			l.log(Level.INFO, "Total time for mapreduce job -> " + elapsed);

			return 0;
		} else {
			return 1;
		}

	}

	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File("//home/mark//Desktop//Big_data/amicable"));
		AmicableJob j = new AmicableJob();
		int rc = ToolRunner.run(j, args);
		System.exit(rc);

	}

}
