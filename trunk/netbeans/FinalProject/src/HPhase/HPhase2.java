package HPhase;

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

    /* The output values must be text in order to distinguish the different data types */
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, MatrixVector> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String[] input = value.toString().split("\t");
	    //System.out.println(input.length + " @ "+input[0] + " ! " + input[1]);
	    int column = 0;
	    //System.out.println(input[0].length() + " $$$ " + input[0].trim());
	    try {
		column = Integer.parseInt(input[0]);
	    } catch (NumberFormatException e) {
		System.out.println("Problem parsing the key: "+input[0].trim());
		throw new IOException(e.toString());
	    }
	    context.write(new IntWritable(column), MatrixVector.parseLine(input[1]));

	}
    }

	public static class MyReducer extends Reducer<IntWritable, MatrixVector, IntWritable, MatrixVector> {

		public void reduce(IntWritable key, Iterable<MatrixVector> values, Context context) throws IOException, InterruptedException
		{
			/* The array contains the the row vector once the w row vector is read */
			MatrixVector mv,result = null;

			Iterator<MatrixVector> iterator = values.iterator();
			if(iterator.hasNext())
			{
				mv = iterator.next();
				result = new MatrixVector(mv.getNumberOfElement(),mv.getValues().clone());
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
		Configuration conf = new Configuration();

		Job job = new Job(conf, "MapRed Step2");
		job.setJarByClass(HPhase2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MatrixVector.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MatrixVector.class);

		job.setNumReduceTasks(2);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

