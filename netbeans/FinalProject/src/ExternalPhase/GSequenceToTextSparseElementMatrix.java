
package ExternalPhase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.GenericElement;
import util.SparseElement;
import util.SparseVectorElement;



public class GSequenceToTextSparseElementMatrix
{
	public static class MyMapper extends Mapper<IntWritable, GenericElement, NullWritable, SparseElement> {

		@Override
		public void map(IntWritable key, GenericElement value, Context context) throws IOException, InterruptedException
		{
                    /*
			SparseElement se = SparseElement.parseLine(value.toString());
			SparseVectorElement sve = new SparseVectorElement(se.getColumn(), se.getValue());
			GenericElement gw = new GenericElement();
			gw.set(sve);
                      */



                        SparseVectorElement sve = (SparseVectorElement) value.get();
                        SparseElement se = new SparseElement(key.get(), sve.getCoordinate(), sve.getValue());


                        context.write(NullWritable.get(), se);

			//context.write(NullWritable.get()), se.toString());
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception
	{
		if(args.length != 2)
		{
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First Parameter: A/W files directories");
			System.err.println("Second Parameter: Output directory");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Translator from Text to Sequence for the Sparse Element");
		job.setJarByClass(TextToGSequenceSparseElementTranslator.class);
		job.setMapperClass(MyMapper.class);

		job.setNumReduceTasks(0);
                
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(SparseElement.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}



}
