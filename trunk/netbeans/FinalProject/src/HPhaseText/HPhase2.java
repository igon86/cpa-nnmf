package HPhaseText;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*; //Text
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.*;
/**
 *
 * @author virgilid
 */
public class HPhase2{

    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, NMFVector> {

        		@Override
		protected void setup(Context context) throws IOException
		{
                    NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		
		}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String[] input = value.toString().split("\t");
	    //System.out.println(input.length + " @ "+input[0] + " ! " + input[1]);
	    int column = 0;
	    //System.out.println(input[0].length() + " $$$ " + input[0].trim());
	    try {
		column = Integer.parseInt(input[0]);
	    } catch (NumberFormatException e) {
		System.err.println("Problem parsing the key: "+input[0].trim());
		throw new IOException(e.toString());
	    }
	    context.write(new IntWritable(column), NMFVector.parseLine(input[1]));

	}
    }

	public static class MyReducer extends Reducer<IntWritable, NMFVector, IntWritable, NMFVector> {

            		@Override
		protected void setup(Context context) throws IOException
		{
                    NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}

		public void reduce(IntWritable key, Iterable<NMFVector> values, Context context) throws IOException, InterruptedException
		{
			/* The array contains the the row vector once the w row vector is read */
			NMFVector mv,result = null;

			Iterator<NMFVector> iterator = values.iterator();
			if(iterator.hasNext())
			{
				mv = iterator.next();
				result = new NMFVector(mv.getNumberOfElement(),mv.getValues().clone());
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
		if(args.length != 3)
		{
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First Parameter: HPhase1 output files directories");
			System.err.println("Second Parameter: Output directory");
                        System.err.println("Third Parameter: K ");
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
		job.setOutputValueClass(NMFVector.class);

		// Testing Job Options
		job.setNumReduceTasks(2);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

