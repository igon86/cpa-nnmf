/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ExternalPhase;


import java.io.IOException;

import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import util.GenericElement;
import util.NMFVector;


public class TextToGSequenceMatrixWHTranslator
{

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, GenericElement> {
		protected void setup(Context context){
		    		    NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] values = value.toString().split("\t");
			Integer index = new Integer(values[0]);
			GenericElement gw = new GenericElement();
			gw.set(NMFVector.parseLine(values[1]));
			context.write(new IntWritable(index),gw );
		}
	}


        public static class MyReducer extends Reducer<IntWritable, GenericElement, IntWritable, GenericElement> {

        @Override
            protected void setup(Context context){
                    NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}
		
        @Override
		public void reduce(IntWritable key, Iterable<GenericElement> values, Context context) throws IOException, InterruptedException
		{

                    Iterator<GenericElement> iter = values.iterator();

                    while( iter.hasNext())
                        context.write(key, iter.next());
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
			System.err.println("First Parameter: A/W files directories");
			System.err.println("Second Parameter: Output directory");
			System.err.println("Third Parameter: The factorizing parameter of the NNMF (K)");
			System.err.println("Fourth Parameter: reduce number");

                        System.exit(-1);

		}

		Configuration conf = new Configuration();
		conf.setInt("elementsNumber", Integer.parseInt(args[2]));
		Job job = new Job(conf, "Translator from Text to Sequence for the H/W Matrix");
		job.setJarByClass(TextToSequenceMatrixWHTranslator.class);
		job.setMapperClass(MyMapper.class);
                job.setReducerClass(MyReducer.class);

		job.setNumReduceTasks(new Integer(args[3]));

                job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(GenericElement.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(GenericElement.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
