package HPhaseText;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.IntAndIdWritable;
import util.NMFVector;
import util.SparseElement;
import util.SparseVectorElement;


/**
 *
 * @author virgilid
 */
public class HPhase1 {

	private static boolean W = false;
	//private static int currentRow;

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<LongWritable, Text, IntAndIdWritable, Text> {

		@Override
		protected void setup(Context context) throws IOException
		{
                    NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
			String folderName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();

			if (folderName.startsWith("W")) /* A row vector must be emitted */
			{
				W = true;
                        }
			else if( ! folderName.startsWith("A")) throw new IOException("File name not correct");
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{

			if (W)
			{
				String[] values = value.toString().split("\t");
                                //System.out.println("Letto vettore riga "+values[0]+ " contenente "+values[1]);
				context.write(new IntAndIdWritable(values[0],'W'), new Text(values[1]) );
			}
			else  /* The sparse element must be emitted */
			{
				SparseElement se = new SparseElement(value);
                                //System.out.println("Letto SE: "+se.toString());
				SparseVectorElement sve = new SparseVectorElement(se.getColumn(), se.getValue());
                                //System.out.println("Creato SVE: "+sve.toString());
				context.write(new IntAndIdWritable(se.getRow(),'a'), new Text(sve.toString()));
			}
		}
//lower case is usefull for the ordering of the key

	}

	public static class MyReducer extends Reducer<IntAndIdWritable, Text, IntWritable, NMFVector> {
                protected void setup(Context context){
		    		    NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}
		@Override
		public void reduce(IntAndIdWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
                        //System.out.println("REDUCE KEY:" +key);

			NMFVector mv = null;

			Text val;

			Iterator<Text> iter = values.iterator();

			if(iter.hasNext())
			{
				val = iter.next();
				//System.out.println("VALUE:"+val);
                                try{
                                    mv = NMFVector.parseLine(val.toString());
                                }
                                catch(IOException e){
                                    System.err.println("Problemi nella PARSELINE di nmfVector");
                                }
			}
			else throw new IOException("It shouldn't be never verified");

			while (iter.hasNext())
			{
				val = iter.next();

				SparseVectorElement sve = SparseVectorElement.parseLine(val.toString());

				if (sve.getValue() != 0.0)
				{
					NMFVector mvEmit =  mv.ScalarProduct(sve.getValue());
					context.write(new IntWritable(sve.getCoordinate()), mvEmit);
				}
			}
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
			System.err.println("First/Second Parameter: A/W files directories");
			System.err.println("Third Parameter: Output directory");
                        System.err.println("Fourth Parameter: The factorizing parameter of the NNMF (K)");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
                conf.setInt("elementsNumber", Integer.parseInt(args[3]));

		Job job = new Job(conf, "MapRed Step1 TEXT");
		job.setJarByClass(HPhase1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(IntAndIdWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NMFVector.class);

		//job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(IntWritable.Comparator.class);

		// Testing Job Options
		job.setNumReduceTasks(2);
		//job.setOutputValueGroupingComparator(Class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}
}

