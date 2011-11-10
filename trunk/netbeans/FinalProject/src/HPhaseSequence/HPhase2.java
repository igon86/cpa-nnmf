package HPhaseSequence;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*; //Text
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.*;
/**
 *
 * @author virgilid
 */
public class HPhase2{

	public static class MyReducer extends Reducer<IntWritable, MatrixVector, IntWritable, MatrixVector> {

		public void reduce(IntWritable key, Iterable<MatrixVector> values, Context context) throws IOException, InterruptedException
		{
			/* The array contains the the row vector once the w row vector is read */
			MatrixVector mv,result = null;

			Iterator<MatrixVector> iterator = values.iterator();
			if(iterator.hasNext())
			{
				mv = iterator.next();
				result = new MatrixVector(mv.getNumberOfElement(),mv.getValues().clone());
			}

			while(iterator.hasNext())
			{
				mv = iterator.next();
				result.inPlaceSum(mv);
			}

			context.write(key,result);
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

		Job job = new Job(conf, "MapRed Step2");
		job.setJarByClass(HPhase2.class);
		//job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		//job.setMapOutputKeyClass(IntWritable.class);
		//job.setMapOutputValueClass(MatrixVector.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MatrixVector.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		// Testing Job Options
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

