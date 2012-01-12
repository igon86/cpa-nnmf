package HPhaseSequence;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import util.*;

public class HPhase2 {

	public static class MyMapper extends Mapper<IntWritable, NMFVector, IntWritable, NMFVector> {

		@Override
		protected void setup(Context context) {
			NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}
	}

	public static class MyCombiner extends Reducer<IntWritable, NMFVector, IntWritable, NMFVector> {

		@Override
		protected void setup(Context context) {
			NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}

		@Override
		public void reduce(IntWritable key, Iterable<NMFVector> values, Context context) throws IOException, InterruptedException {
			/* The array contains the the row vector once the w row vector is read */
			NMFVector mv, result = null;

			Iterator<NMFVector> iterator = values.iterator();
			if (iterator.hasNext()) {
				mv = iterator.next();
				result = new NMFVector(mv.getNumberOfElement(), mv.getValues().clone());
			}

			while (iterator.hasNext()) {
				mv = iterator.next();
				result.inPlaceSum(mv);
			}
			context.write(key, result);
		}
	}

	public static class MyReducer extends Reducer<IntWritable, NMFVector, IntWritable, GenericElement> {

		@Override
		protected void setup(Context context) {
			NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}

		@Override
		public void reduce(IntWritable key, Iterable<NMFVector> values, Context context) throws IOException, InterruptedException {
			/* The array contains the the row vector once the w row vector is read */
			NMFVector mv, result = null;

			Iterator<NMFVector> iterator = values.iterator();
			if (iterator.hasNext()) {
				mv = iterator.next();
				result = new NMFVector(mv.getNumberOfElement(), mv.getValues().clone());
			}

			while (iterator.hasNext()) {
				mv = iterator.next();
				result.inPlaceSum(mv);
			}
			GenericElement out = new GenericElement();
			out.set(result);
			context.write(key, out);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First Parameter: HPhase1 output files directories");
			System.err.println("Second Parameter: Output directory");
			System.err.println("Third Parameter: The factorizing parameter of the NNMF (K)");
			System.err.println("Fourth Parameter: reduce number");

			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.setInt("elementsNumber", Integer.parseInt(args[2]));

		Job job = new Job(conf, "MapRed Step2");
		job.setJarByClass(HPhase2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NMFVector.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(GenericElement.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setCombinerClass(MyCombiner.class);

		job.setNumReduceTasks(new Integer(args[3]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

