
package HPhaseSequence;

import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.*;

/**
 *
 * @author virgilid
 */
public class HPhase3 {

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<IntWritable, MatrixVector, IntWritable, MatrixMatrix> {

		@Override
		public void map(IntWritable key, MatrixVector value, Context context) throws IOException, InterruptedException
		{

			MatrixMatrix result = value.externalProduct(value);

			System.out.println("External Prod = "+result.toString());

			context.write(new IntWritable(0), result);

		}

	}

	/**
	 * null writable is used in order to serialize a MatrixMatrix only
	 */
	public static class MyReducer extends Reducer<IntWritable, MatrixMatrix, NullWritable, MatrixMatrix> {

		@Override
		public void reduce(IntWritable key, Iterable<MatrixMatrix> values, Context context) throws IOException, InterruptedException
		{
			MatrixMatrix result;

			Iterator<MatrixMatrix> iter = values.iterator();
			MatrixMatrix val;

			if(iter.hasNext())
			{
				val = iter.next();
				result = new MatrixMatrix(val.getRowNumber(), val.getColumnNumber(), val.getValues().clone());
				System.out.println("REDUCE: ho ricevuto: "+result.toString());
			}
			else throw new IOException("It shouldn't be never verified");


			while (iter.hasNext())
			{
				val = iter.next();
				System.out.println("REDUCE: ho ricevuto: "+val.toString());
				if (!result.inPlaceSum(val)){
				    System.out.println("ERRORE nella somma di matrici");
				    throw new IOException("ERRORE nella somma di matrici");
				}
			}

			context.write(NullWritable.get(), result);
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
			System.err.println("First Parameter: W files directories");
			System.err.println("Second Parameter: Output directory");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "MapRed Step3");
		job.setJarByClass(HPhase3.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// Testing Job Options
		//job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MatrixMatrix.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(MatrixMatrix.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

