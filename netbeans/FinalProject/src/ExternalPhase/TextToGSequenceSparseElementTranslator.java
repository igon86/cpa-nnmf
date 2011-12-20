
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
import util.SparseElement;
import util.SparseVectorElement;

public class TextToGSequenceSparseElementTranslator
{
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, GenericElement> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			SparseElement se = SparseElement.parseLine(value.toString());
			SparseVectorElement sve = new SparseVectorElement(se.getColumn(), se.getValue());
			GenericElement gw = new GenericElement();
			gw.set(sve);
			context.write(new IntWritable(se.getRow()), gw);
		}
	}

        public static class MyReducer extends Reducer<IntWritable, GenericElement, IntWritable, GenericElement> {

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
		if(args.length != 3)
		{
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First Parameter: A/W files directories");
			System.err.println("Second Parameter: Output directory");
                	System.err.println("Third Parameter: reduce number");

			System.exit(-1);
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Translator from Text to Sequence for the Sparse Element");
		job.setJarByClass(TextToGSequenceSparseElementTranslator.class);
		job.setMapperClass(MyMapper.class);
                job.setReducerClass(MyReducer.class);

		job.setNumReduceTasks(new Integer(args[2]));

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
