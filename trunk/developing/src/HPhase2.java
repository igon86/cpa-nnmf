
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*; //Text
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author virgilid
 */
public class HPhase2{

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<Text, Text, IntWritable, Text> {
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException 
		{
			int keyValue = Integer.parseInt(key.toString());
			context.write(new IntWritable(keyValue), value); 
		}

	}
	
	public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{	
			/* The array contains the the row vector once the w row vector is read */
			Double[] dValues = null, dResults = null;
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
		job.setJarByClass(HPhase1.class);
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
