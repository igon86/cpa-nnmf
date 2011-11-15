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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import util.*;
/**
 *
 * @author virgilid
 */
public class HPhase2{

	public static class MyMapper extends Mapper<IntWritable, MatrixVector, IntWritable, GenericWritablePhase1> {
		protected void setup(Context context){
		    		    MatrixVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}
		@Override
		public void map(IntWritable key, MatrixVector value, Context context) throws IOException, InterruptedException
		{
			/**wraps the matrix vector */
			GenericWritablePhase1 out = new GenericWritablePhase1();
			out.set(value);

			context.write(key, out);

		}

	}

	public static class MyReducer extends Reducer<IntWritable, GenericWritablePhase1, IntWritable, GenericWritablePhase1> {
		protected void setup(Context context){
		    		    MatrixVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}
		public void reduce(IntWritable key, Iterable<GenericWritablePhase1> values, Context context) throws IOException, InterruptedException
		{
			/* The array contains the the row vector once the w row vector is read */
			MatrixVector mv,result = null;

			Iterator<GenericWritablePhase1> iterator = values.iterator();
			if(iterator.hasNext())
			{
				mv = (MatrixVector) iterator.next().get();
				result = new MatrixVector(mv.getNumberOfElement(),mv.getValues().clone());
			}

			while(iterator.hasNext())
			{
				mv = (MatrixVector) iterator.next().get();
				result.inPlaceSum(mv);
			}
			GenericWritablePhase1 out = new GenericWritablePhase1();
			out.set(result);
			context.write(key,out);
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception
	{
		if(args.length != 3)
		{
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First Parameter: HPhase1 output files directories");
			System.err.println("Second Parameter: Output directory");
			System.err.println("Third Parameter: The factorizing parameter of the NNMF (K)");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
				conf.setInt("elementsNumber", Integer.parseInt(args[2]));

		Job job = new Job(conf, "MapRed Step2");
		job.setJarByClass(HPhase2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(GenericWritablePhase1.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(GenericWritablePhase1.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setCombinerClass(MyReducer.class);
		// Testing Job Options
		job.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

