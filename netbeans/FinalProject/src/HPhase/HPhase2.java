

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
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
                        String[] input = value.toString().split("\t");
                        //System.out.println(input.length + " @ "+input[0] + " ! " + input[1]);
                        try{
                             System.out.println(input[0].length() + " $$$ " +input[0].trim());
                            int column = Integer.parseInt(input[0]);
                           context.write(new IntWritable(column), new Text(input[1]));
                        }catch(Exception e){
                            System.out.println(input[0].trim());
                        }

                        for (int i=0; i<input.length; i++){
                            System.out.println(input[i]);
                        }
		}

	}

	public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			/* The array contains the the row vector once the w row vector is read */
			double[] dValues = null, dResults = null;
			MatrixVector mv = null;

			Iterator<Text> iterator = values.iterator();
			if(iterator.hasNext())
			{
				mv = new MatrixVector(iterator.next());
				dResults = mv.getValues();
			}

			while(iterator.hasNext())
			{
				mv = new MatrixVector(iterator.next());
				dValues = mv.getValues();
				for(int i=0; i<dValues.length; i++)
				{
					dResults[i] += dValues[i];
				}
			}

			MatrixVector result = new MatrixVector(dResults.length, dResults);
			context.write(key, new Text(result.toString()));			}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		Job job = new Job(conf, "MapRed Step1");
		job.setJarByClass(HPhase2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		//job.setOutputValueGroupingComparator(Class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

