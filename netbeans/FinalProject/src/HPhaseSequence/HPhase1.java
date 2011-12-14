package HPhaseSequence;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import util.GenericElement;
import util.IntAndIdWritable;
import util.NMFVector;
import util.SparseVectorElement;


/**
 *
 * @author virgilid
 */
public class HPhase1 {

	private static boolean W = false;
	//private static int currentRow;

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<IntWritable, GenericElement, IntAndIdWritable, GenericElement> {

		@Override
		protected void setup(Context context) throws IOException
		{

		    NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));

			String folderName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();


			/*  the number present in the file name is the number of the first stored row vector
                            Through a static variable we take into account the right row number knowing that the
                            row vector are read sequentially in the file split
			*/

			if (folderName.startsWith("W")) /* A row vector must be emitted */
			{
				//int i,j;

				W = true;
				/**
				//for (i = 0; i < chunkName.length() &&
					(chunkName.charAt(i) < '0' || chunkName.charAt(i) > '9'); i++);

				//for (j=i; j < chunkName.length() &&
				 (chunkName.charAt(j) >= '0' && chunkName.charAt(j) <= '9'); j++);

				//try
				{
					String rowNumber = chunkName.substring(i, j);
					System.out.println("GUARDARE:" + rowNumber);
					currentRow = new Integer(rowNumber);
				}
				catch (NumberFormatException e) { throw new IOException("File name conversion failled"); }
			*/}
			else if( ! folderName.startsWith("A")) throw new IOException("File name not correct");
		}

		@Override
		public void map(IntWritable key, GenericElement value, Context context) throws IOException, InterruptedException
		{

			if (W)
			{
				
				context.write(new IntAndIdWritable(key.get(),'W'), value );
			}
			else  /* The sparse element must be emitted */
			{

				context.write(new IntAndIdWritable(key.get(),'a'), value);
			}
		}
                //lower case is usefull for the ordering of the key

	}



	  /**
	   * Partition based on the first part of the pair.
	   */
	  public static class FirstPartitioner extends Partitioner<Text,Text>{
	    @Override
	    public int getPartition(Text key, Text value, int numPartitions) {
	    	int j;
	    	String s = key.toString();
	    	for (j=0; j < s.length() && (s.charAt(j) >= '0' && s.charAt(j) <= '9'); j++);
	    	int parsed = Integer.parseInt(s.substring(0, j));
		System.out.println("Invocato FirstPartitioner con KEY: "+key.toString()+"\nVALUE: "
			+value.toString()+"\nnumPartitions: "+numPartitions +"\n e lo mando al reducer: "+parsed%numPartitions);
	      return parsed % numPartitions;
	    }
          }

	  /**
	   * Compare only the first part of the pair, so that reduce is called once
	   * for each value of the first part.
	   */
	  public static class FirstGroupingComparator implements RawComparator<Text> {

		@Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
	    {
		System.out.println("Sono nella GroupingComparator");
                DataInputBuffer buffer = new DataInputBuffer();
                WritableComparable t1 = new Text();
                WritableComparable t2 = new Text();
                try {
                    buffer.reset(b1, s1, l1);
                    t1.readFields(buffer);
                    System.out.println("ARG1: "+t1.toString());
                    buffer.reset(b2, s2, l2);
                    t2.readFields(buffer);
                    System.out.println("ARG2: "+t2.toString());

                } catch (IOException e) {
                    System.out.println("col cazzo che ha funzionato");
                    throw new RuntimeException(e);
                }
	    	return compare((Text )t1, (Text) t2);


	    }


		@Override
	    public int compare(Text o1, Text o2)
	    {

	      String s1,s2;
	      s1 = o1.toString().substring(0, o1.getLength()-1);
              s2 = o2.toString().substring(0, o2.getLength()-1);
              System.out.println("sono nella compare oggetti: "+s1+" VS "+s2);
	      return s1.compareTo(s2);
	    }
	  }

	public static class MyReducer extends Reducer<IntAndIdWritable, GenericElement, IntWritable, NMFVector> {
		protected void setup(Context context){
		    		    NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
		}
		@Override
		public void reduce(IntAndIdWritable key, Iterable<GenericElement> values, Context context) throws IOException, InterruptedException
		{
                        //System.out.println("REDUCE KEY:" +key);
			
			NMFVector mv = null,temp = null;

			SparseVectorElement val = null;

			Iterator<GenericElement> iter = values.iterator();

			if(iter.hasNext())
			{
                                try{
                                    temp = (NMFVector) iter.next().get();
                                }
                                catch (ClassCastException e){
                                    System.out.println("Problemi nel SORT della FASE 1 per la key "+key.toString()+"\n"+e.toString());
                                }
				mv = new NMFVector(temp.getNumberOfElement(), temp.getValues().clone());
				//System.out.println("VETTORE: "+mv.toString());

			}
			while (iter.hasNext())
			{
				val = (SparseVectorElement) iter.next().get();
				//System.out.println("SPARSE ELEMENT" + val.toString());
				if (val.getValue() != 0.0)
				{
					NMFVector mvEmit =  mv.ScalarProduct(val.getValue());
					context.write(new IntWritable(val.getCoordinate()), mvEmit);
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

		Job job = new Job(conf, "MapRed Step1");
		job.setJarByClass(HPhase1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(IntAndIdWritable.class);
		job.setMapOutputValueClass(GenericElement.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NMFVector.class);

		//job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(IntWritable.Comparator.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// Testing Job Options
		job.setNumReduceTasks(2);
		//job.setOutputValueGroupingComparator(Class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}
}

