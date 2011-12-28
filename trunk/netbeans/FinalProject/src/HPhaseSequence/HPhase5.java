
package HPhaseSequence;

import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import util.*;

/**
 *
 * @author virgilid
 */
public class HPhase5 {
    

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<IntWritable, GenericElement, IntAndIdWritable, NMFVector> {

		char matrixId;
		@Override
		protected void setup(Context context) throws IOException
		{
		    	NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));

			String folderName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();

			matrixId = folderName.charAt(0);
		}

		@Override
		public void map(IntWritable key, GenericElement value, Context context) throws IOException, InterruptedException
		{
			    context.write(new IntAndIdWritable(key.get(),matrixId), (NMFVector) value.get());	
		}

	}

	/**
	 * null writable is used in order to serialize a MatrixMatrix only
	 */
	public static class MyReducer extends Reducer<IntAndIdWritable, NMFVector, IntWritable, GenericElement> {
	@Override
		protected void setup(Context context){
			NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}
		@Override
		public void reduce(IntAndIdWritable key, Iterable<NMFVector> values, Context context) throws IOException, InterruptedException
		{
			// reduce should receive H,X,Y vector exactly in this order AND nothing else
			NMFVector[] vectors = new NMFVector[3];
			NMFVector val = null;

			Iterator<NMFVector> iter = values.iterator();

			int i = 0;
			while (iter.hasNext() && i <3)
			{
				val = iter.next();
				vectors[i++] = new NMFVector(val.getNumberOfElement(), val.getValues().clone());

			}
			if (iter.hasNext() || i<3){ // this must throw an Exception
			    System.err.println("SONO il reducer della key: " +key.toString() + " e ho ricevuto " +i +" valori");
			}
			else{
			    vectors[0].inPlacePointMul(vectors[1]);
			    vectors[0].inPlacePointDiv(vectors[2]);
			    GenericElement gw = new GenericElement();
			    gw.set(vectors[0]);
			    context.write(new IntWritable(key.get()), gw);
			}
			
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception
	{
		if(args.length != 6)
		{
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First/Second/Third Parameter: "
					+ "H/X/Y files directories");
			System.err.println("Fourth Parameter: Output directory");
			System.err.println("Fiveth Parameter: The factorizing parameter of the NNMF (K)");
                        System.err.println("Sixth Parameter: reduce number");

			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.setInt("elementsNumber", Integer.parseInt(args[4]));

		Job job = new Job(conf, "MapRed Step5");
		job.setJarByClass(HPhase5.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(IntAndIdWritable.class);
		job.setMapOutputValueClass(NMFVector.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(GenericElement.class);

		job.setGroupingComparatorClass(IntWritable.Comparator.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Testing Job Options
		job.setNumReduceTasks(new Integer(args[5]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[2]));

		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	}
}

