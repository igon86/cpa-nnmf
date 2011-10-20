
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*; //Text
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author virgilid
 */
public class HPhase1 {
	
	private static boolean W = false;
	private static int currentRow;

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		protected void setup(Context context) throws IOException
		{
			String chunkName = ((FileSplit) context.getInputSplit()).getPath().getName();

			/* the number present in the file name is the number of the first stored row vector
			// Through a static variable we take in account the right row number knowing that the
			// row vector are read sequentially in the file split 
			*/
			
			if (chunkName.startsWith("W")) /* A row vector must be emitted */
			{
				int i,j;
				
				W = true;
				
				for (i = 0; i < chunkName.length() &&
					(chunkName.charAt(i) < '0' || chunkName.charAt(i) > '9'); i++);
				
				for (j=i; i < chunkName.length() &&
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
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{

			if (W)
			{
				context.write(new IntWritable(currentRow++), new Text("W" + value.toString()));
			} 
			else  /* The sparse element must be emitted */
			{
				SparseElement se = new SparseElement(value);
				SparseVectorElement sve = new SparseVectorElement(se.getColumn(), se.getValue());
				context.write(new IntWritable(se.getRow()), new Text("A" + sve.toString())); 
			}
		}

	}

	public static class MyRawComparator extends Text.Comparator{
		
	}
	
	public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		private void scalarProductEmit(Double[] dValues, SparseVectorElement tmp, Context context) throws IOException, InterruptedException 
		{
			if (tmp.getValue() != 0.0) 
			{
				Double[] doubleTmp = dValues.clone();
				for (int j = 0; j < doubleTmp.length; j++)
				{
					doubleTmp[j] *= tmp.getValue();
				}

				MatrixVector mvEmit = new MatrixVector(dValues.length, doubleTmp);

				context.write(new IntWritable(tmp.getCoordinate()), new Text(mvEmit.toString()));
			}
		}

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{	
			/* The array contains the the row vector once the w row vector is read */
			Double[] dValues = null;

			/* The array list contains the sparse element on the key-th row */
			ArrayList<SparseVectorElement> arrList = new ArrayList<SparseVectorElement>();

			Text val;
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()) 
			{
				val = iter.next();

				if (val.charAt(0) == 'A') // / MULTIPLE A ELEMENTS VALUE ARE NOT TAKEN IN ACCOUNT
				{
					SparseVectorElement sve = SparseVectorElement.parseLine(val.toString().substring(1));
					arrList.add(sve);
				} 
				else if (val.charAt(0) == 'W') 
				{
					MatrixVector mv = MatrixVector.parseLine(val.toString().substring(1));
					dValues = mv.getValues();

					System.out.println("Vettore aquisito");
					
					for (SparseVectorElement tmp : arrList) 
					{
						scalarProductEmit(dValues, tmp, context);
					}

					/* Exit from the iterator loop */
					break;
				}
			}

			if (dValues == null)
				throw new IOException("The W's row vector is not received");			

			System.out.println("QUI CI ARRIVO");
			while (iter.hasNext()) 
			{
				val = iter.next();

				if (val.charAt(0) == 'A') // MULTIPLE A ELEMENTS VALUE ARE NOT TAKEN IN ACCOUNT
				{
					SparseVectorElement sve = SparseVectorElement.parseLine(val.toString().substring(1));
					scalarProductEmit(dValues, sve, context);
				} 
				else if (val.charAt(0) == 'W')
					throw new IOException("There is a double emission of the W's row vector");

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

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
				
		//job.setOutputValueGroupingComparator(Class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
