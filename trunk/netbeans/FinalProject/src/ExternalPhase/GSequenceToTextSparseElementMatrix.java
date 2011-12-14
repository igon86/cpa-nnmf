
package ExternalPhase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
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
	public static class MyMapper extends Mapper<IntWritable, GenericElement, SparseElement, NullWritable> {

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

                        try{
                            se.toString();
                        }
                        catch(NullPointerException e)
                        {
                            System.out.println("!!!####\n");
                            System.out.println("!!!####Errore di null pointer " + key.get() +"\\" + sve.getCoordinate()+"\\"+sve.getValue()+"\n");
                        }
                        


                        context.write(se, NullWritable.get());

			//context.write(NullWritable.get()), se.toString());
		}
	}

/*

        public static class MyReducer extends Reducer<NullWritable, SparseElement, NullWritable, SparseElement> {

		
		public void reduce(NullWritable key, SparseElement value, Context context) throws IOException, InterruptedException
		{

                        try{
                            value.toString();
                            context.write(NullWritable.get(), value);
                        }
                        catch(NullPointerException e)
                        {
                            System.out.println("!!!####\n");
                            System.out.println("!!!####Errore di null pointer " + (value == null) +"\n");
                        }


		}
	}
*/

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
                //job.setReducerClass(MyReducer.class);

		job.setNumReduceTasks(0);

                //job.setMapOutputKeyClass(NullWritable.class);
                //job.setMapOutputValueClass(SparseElement.class);
		job.setOutputKeyClass(SparseElement.class);
		job.setOutputValueClass(NullWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}



}
