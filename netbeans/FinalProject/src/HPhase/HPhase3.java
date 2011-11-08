
package HPhase;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.*;

/**
 *
 * @author virgilid
 */
public class HPhase3 {

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, MatrixMatrix> {

		@Override
		protected void setup(Context context) throws IOException
		{
			String chunkName = ((FileSplit) context.getInputSplit()).getPath().getName();

			/* the number present in the file name is the number of the first stored row vector
			// Through a static variable we take in account the right row number knowing that the
			// row vector are read sequentially in the file split
			*/

			if (!chunkName.startsWith("W"))
			{
				throw new IOException("File name is not correct: "+chunkName);
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String vector = value.toString().split("\t")[1];
			
			MatrixVector mv = MatrixVector.parseLine(vector);

			MatrixMatrix result = mv.externalProduct(mv);

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
		Configuration conf = new Configuration();

		Job job = new Job(conf, "MapRed Step3");
		job.setJarByClass(HPhase3.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		//job.setNumReduceTasks(0);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MatrixMatrix.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		//TextOutputFormat.setOutputPath(job, new Path(args[1]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

