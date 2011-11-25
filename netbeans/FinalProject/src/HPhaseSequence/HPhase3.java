
package HPhaseSequence;

import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import util.*;

/**
 *
 * @author virgilid
 */
public class HPhase3 {

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<IntWritable, GenericElement, NullWritable, NMFMatrix> {
	@Override
		protected void setup(Context context){
			NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}
		@Override
		public void map(IntWritable key, GenericElement value, Context context) throws IOException, InterruptedException
		{
			NMFVector mv = (NMFVector) value.get();
			NMFMatrix result = mv.externalProduct(mv);

			System.out.println("External Prod = "+result.toString());

			context.write(NullWritable.get(), result);

		}

	}

	/**
	 * null writable is used in order to serialize a MatrixMatrix only
	 */
	public static class MyReducer extends Reducer<NullWritable, NMFMatrix, NullWritable, NMFMatrix> {
	@Override
		protected void setup(Context context){
			NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}
		@Override
		public void reduce(NullWritable key, Iterable<NMFMatrix> values, Context context) throws IOException, InterruptedException
		{
			NMFMatrix result;

			Iterator<NMFMatrix> iter = values.iterator();
			NMFMatrix val;

			if(iter.hasNext())
			{
				val = iter.next();
				result = new NMFMatrix(val.getRowNumber(), val.getColumnNumber(), val.getValues().clone());
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
			System.out.println(result.toString());
			context.write(NullWritable.get(), result);
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
			System.err.println("First Parameter: W files directories");
			System.err.println("Second Parameter: Output directory");
			System.err.println("Third Parameter: The factorizing parameter of the NNMF (K)");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.setInt("elementsNumber", Integer.parseInt(args[2]));
		Job job = new Job(conf, "MapRed Step3");
		job.setJarByClass(HPhase3.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setCombinerClass(MyReducer.class);

		// Testing Job Options
		//job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NMFMatrix.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NMFMatrix.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

