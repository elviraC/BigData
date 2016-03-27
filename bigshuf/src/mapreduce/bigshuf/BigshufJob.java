package mapreduce.bigshuf;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BigshufJob extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "Dotdelimiter Job");
        job.setJarByClass(getClass());

        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/home/mark/Desktop/Big_data/bigshuf.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/home/mark/Desktop/Big_data/bigshuf"));

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(BigshuffMapper.class);
        job.setNumReduceTasks(0);

        conf.set("textinputformat.record.delimiter", ".");

        return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File("//home/mark//Desktop//Big_data/bigshuf"));
		BigshufJob j = new BigshufJob();
		int rc = ToolRunner.run(j, args);
		System.exit(rc);

	}

}
