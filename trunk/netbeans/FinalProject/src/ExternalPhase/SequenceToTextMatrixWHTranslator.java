/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ExternalPhase;

import util.NMFVector;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SequenceToTextMatrixWHTranslator
{
	public static class MyMapper extends Mapper<IntWritable, NMFVector, IntWritable, NMFVector> {

		@Override
		public void map(IntWritable key, NMFVector values, Context context) throws IOException, InterruptedException
		{
			context.write(key, values);
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
			System.err.println("First Parameter: HPhase1 output files directories");
			System.err.println("Second Parameter: Output directory");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Translator from Sequence to Text for the H/W Matrix");
		job.setJarByClass(SequenceToTextMatrixWHTranslator.class);
		job.setMapperClass(MyMapper.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NMFVector.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
