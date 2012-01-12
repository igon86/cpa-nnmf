package HPhaseSequence;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import util.GenericElement;
import util.IntAndIdWritable;
import util.NMFVector;
import util.SparseVectorElement;

public class HPhase1 {

	private static boolean W = false;
	//private static int currentRow;

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<IntWritable, GenericElement, IntAndIdWritable, GenericElement> {

		@Override
		protected void setup(Context context) throws IOException {

			NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));

			String folderName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();


			/*  the number present in the file name is the number of the first stored row vector
			Through a static variable we take into account the right row number knowing that the
			row vector are read sequentially in the file split
			 */

			if (folderName.startsWith("W")) /* A row vector must be emitted */ {

				W = true;
			} else if (!folderName.startsWith("A")) {
				throw new IOException("File name not correct");
			}
		}

		@Override
		public void map(IntWritable key, GenericElement value, Context context) throws IOException, InterruptedException {

			if (W) {
				context.write(new IntAndIdWritable(key.get(), 'W'), value);
			} else /* The sparse element must be emitted */ {

				context.write(new IntAndIdWritable(key.get(), 'a'), value);
			}
		}
		//lower case is usefull for the ordering of the key
	}

	public static class MyReducer extends Reducer<IntAndIdWritable, GenericElement, IntWritable, NMFVector> {

		@Override
		protected void setup(Context context) {
			NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}

		@Override
		public void reduce(IntAndIdWritable key, Iterable<GenericElement> values, Context context) throws IOException, InterruptedException {
			//System.out.println("REDUCE KEY:" +key);

			NMFVector mv = null, temp = null;

			SparseVectorElement val = null;

			Iterator<GenericElement> iter = values.iterator();

			if (iter.hasNext()) {
				GenericElement g = iter.next();
				try {
					temp = (NMFVector) g.get();
				} catch (ClassCastException e) {
					val = (SparseVectorElement) g.get();
					System.err.println("Problemi nel SORT della FASE 1 per la key " + key.toString() + "VALUE: " + val.toString() + "\n" + e.toString());
				}
				mv = new NMFVector(temp.getNumberOfElement(), temp.getValues().clone());

			}
			while (iter.hasNext()) {
				val = (SparseVectorElement) iter.next().get();
				if (val.getValue() != 0.0) {
					NMFVector mvEmit = mv.ScalarProduct(val.getValue());
					context.write(new IntWritable(val.getCoordinate()), mvEmit);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First/Second Parameter: A/W files directories");
			System.err.println("Third Parameter: Output directory");
			System.err.println("Fourth Parameter: The factorizing parameter of the NNMF (K)");
			System.err.println("Fifth Parameter: reduce number");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.setInt("elementsNumber", Integer.parseInt(args[3]));

		Job job = new Job(conf, "H MapRed Step1");
		job.setJarByClass(HPhase1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(IntAndIdWritable.class);
		job.setMapOutputValueClass(GenericElement.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NMFVector.class);

		job.setGroupingComparatorClass(IntWritable.Comparator.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setNumReduceTasks(new Integer(args[4]));

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}
}

