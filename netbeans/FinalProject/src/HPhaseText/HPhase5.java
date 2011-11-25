
package HPhaseText;

import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
public class HPhase5 {

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<LongWritable, Text, IntAndIdWritable, NMFVector> {

		char matrixId;
		@Override
		protected void setup(Context context) throws IOException
		{
			String folderName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();
			System.out.println("FOLDERNAME: " +folderName);
			matrixId = folderName.charAt(0);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			System.out.println("map della fase 5 VALUE" +value.toString());
			//if(value.toString().trim().length() != 0){ //this problem must be solved
			    String[] values = value.toString().split("\t");
			    int column = Integer.parseInt(values[0]);

			    NMFVector out = new NMFVector(new Text(values[1]));

			    context.write(new IntAndIdWritable(column,matrixId), out);
			//}
		}

	}

	/**
	 * null writable is used in order to serialize a MatrixMatrix only
	 */
	public static class MyReducer extends Reducer<IntAndIdWritable, NMFVector, IntWritable, NMFVector> {

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
				//System.out.println("REDUCE: ho ricevuto: "+val.toString());
				//if (!result.inPlaceSum(val)){
				//    System.out.println("ERRORE nella somma di matrici");
				//    throw new IOException("ERRORE nella somma di matrici");
				//}
			}
			if (iter.hasNext() || i<3){ // this must throw an Exception
			    System.out.println("SONO il reducer della key: " +key.toString() + " e ho ricevuto " +i +" valori");
			}
			else{
			    vectors[0].inPlacePointMul(vectors[1]);
			    vectors[0].inPlacePointDiv(vectors[2]);
			    context.write(new IntWritable(key.get()), vectors[0]);
			}
			
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception
	{
		if(args.length != 4)
		{
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First/Second/Third Parameter: "
					+ "H/X/Y files directories");
			System.err.println("Third Parameter: Output directory");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "MapRed Step5");
		job.setJarByClass(HPhase5.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		//job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(IntAndIdWritable.class);
		job.setMapOutputValueClass(NMFVector.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NMFVector.class);

		job.setGroupingComparatorClass(IntWritable.Comparator.class);

		// Testing Job Options


		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextInputFormat.addInputPath(job, new Path(args[1]));
		TextInputFormat.addInputPath(job, new Path(args[2]));

		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	}
}

