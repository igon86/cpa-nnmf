
package HPhaseSequence;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import util.*;

/**
 *
 * @author virgilid
 */
public class HPhase5 {

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<IntWritable, GenericWritablePhase1, IntAndIdWritable, MatrixVector> {

		char matrixId;
		@Override
		protected void setup(Context context) throws IOException
		{
			String folderName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();

			matrixId = folderName.charAt(0);
		}

		@Override
		public void map(IntWritable key, GenericWritablePhase1 value, Context context) throws IOException, InterruptedException
		{
			System.out.println("map della fase 5 VALUE" +value.toString());
			//if(value.toString().trim().length() != 0){ //this problem must be solved
			//    String[] values = value.toString().split("\t");
			//    int column = Integer.parseInt(values[0]);

			//    MatrixVector out = new MatrixVector(new Text(values[1]));
			    context.write(new IntAndIdWritable(key.get(),matrixId), (MatrixVector) value.get());
			//}
		}

	}

	/**
	 * null writable is used in order to serialize a MatrixMatrix only
	 */
	public static class MyReducer extends Reducer<IntAndIdWritable, MatrixVector, IntWritable, GenericWritablePhase1> {

		@Override
		public void reduce(IntAndIdWritable key, Iterable<MatrixVector> values, Context context) throws IOException, InterruptedException
		{
			// reduce should receive H,X,Y vector exactly in this order AND nothing else
			MatrixVector[] vectors = new MatrixVector[3];
			MatrixVector val = null;

			Iterator<MatrixVector> iter = values.iterator();

			int i = 0;
			while (iter.hasNext() && i <3)
			{
				val = iter.next();
				vectors[i++] = new MatrixVector(val.getNumberOfElement(), val.getValues().clone());
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
			    GenericWritablePhase1 gw = new GenericWritablePhase1();
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
		job.setMapOutputValueClass(MatrixVector.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(GenericWritablePhase1.class);

		job.setGroupingComparatorClass(IntWritable.Comparator.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Testing Job Options


		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[2]));

		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	}
}

