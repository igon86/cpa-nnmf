package HPhase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.MatrixVector;
import util.SparseElement;
import util.SparseVectorElement;


/**
 *
 * @author virgilid
 */
public class HPhase1 {

	private static boolean W = false;
	private static int currentRow;

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void setup(Context context) throws IOException
                {
			String chunkName = ((FileSplit) context.getInputSplit()).getPath().getName();

			/*  the number present in the file name is the number of the first stored row vector
                            Through a static variable we take into account the right row number knowing that the
                            row vector are read sequentially in the file split
			*/

			if (chunkName.startsWith("W")) /* A row vector must be emitted */
			{
				int i,j;

				W = true;

				for (i = 0; i < chunkName.length() &&
					(chunkName.charAt(i) < '0' || chunkName.charAt(i) > '9'); i++);

				for (j=i; j < chunkName.length() &&
				 (chunkName.charAt(j) >= '0' && chunkName.charAt(j) <= '9'); j++);

				try
				{
					String rowNumber = chunkName.substring(i, j);
					System.out.println("GUARDARE:" + rowNumber);
					currentRow = new Integer(rowNumber);
				}
				catch (NumberFormatException e) { throw new IOException("File name conversion failled"); }
			}
			else if( ! chunkName.startsWith("A")) throw new IOException("File name not correct");
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{

			if (W)
			{
				context.write(new Text("" + currentRow +"W"  ), new Text(value.toString()));
				currentRow++;
			}
			else  /* The sparse element must be emitted */
			{
				SparseElement se = new SparseElement(value);
				SparseVectorElement sve = new SparseVectorElement(se.getColumn(), se.getValue());
				context.write(new Text("" + se.getRow() + "a"), new Text(sve.toString()));
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
                DataInputBuffer buffer = new DataInputBuffer();
                WritableComparable t1 = new Text();
                WritableComparable t2 = new Text();
                try {
                    buffer.reset(b1, s1, l1);
                    t1.readFields(buffer);
                    System.out.println("STICAZZI1: "+t1.toString());
                    buffer.reset(b2, s2, l2);
                    t2.readFields(buffer);
                    System.out.println("STICAZZI2: "+t2.toString());

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

	public static class MyReducer extends Reducer<Text, Text, IntWritable, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
                        System.out.println("REDUCE KEY:" +key);
			/* The array contains the the row vector once the w row vector is read */
			//double[] dValues = null;
			MatrixVector mv;

			Text val;

			Iterator<Text> iter = values.iterator();

			if(iter.hasNext())
			{
				val = iter.next();
				System.out.println("VALUE:"+val);

				mv = MatrixVector.parseLine(val.toString());
				//dValues = mv.getValues();
			}
			else throw new IOException("It shouldn't be never verified");

			while (iter.hasNext())
			{
				val = iter.next();

				SparseVectorElement sve = SparseVectorElement.parseLine(val.toString());

				if (sve.getValue() != 0.0)
				{
					MatrixVector mvEmit =  mv.ScalarProduct(sve.getValue());
					context.write(new IntWritable(sve.getCoordinate()), new Text(mvEmit.toString()));
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
		Configuration conf = new Configuration();

		Job job = new Job(conf, "MapRed Step1");
		job.setJarByClass(HPhase1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
                //job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);

		//job.setOutputValueGroupingComparator(Class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

