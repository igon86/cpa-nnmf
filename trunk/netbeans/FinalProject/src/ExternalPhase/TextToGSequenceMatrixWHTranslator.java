/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ExternalPhase;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import util.GenericWritablePhase1;
import util.MatrixVector;


public class TextToGSequenceMatrixWHTranslator
{

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, GenericWritablePhase1> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] values = value.toString().split("\t");
			Integer index = new Integer(values[0]);
			GenericWritablePhase1 gw = new GenericWritablePhase1();
			gw.set(MatrixVector.parseLine(values[1]));
			context.write(new IntWritable(index),gw );
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception
	{
		if(args.length != 2)
		{
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First Parameter: A/W files directories");
			System.err.println("Second Parameter: Output directory");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Translator from Text to Sequence for the H/W Matrix");
		job.setJarByClass(TextToSequenceMatrixWHTranslator.class);
		job.setMapperClass(MyMapper.class);

		job.setNumReduceTasks(0);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(GenericWritablePhase1.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
