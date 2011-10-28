
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


/**
 * 
 * @author virgilid
 */
public class HPhase3 {
	
	private static int currentRow;

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, MatrixMatrix> {

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
			else throw new IOException("File name not correct");
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{

			MatrixVector mv = new MatrixVector(value);
			
			int size = mv.getNumberOfElement();
			Double[][] tmp = new Double[size][size];
			Double[] vect = mv.getValues();
			
			for(int i=0; i<size; i++)
			{
				for(int j=0; j<size; j++)
				{
					tmp[i][j] = vect[i] * vect[j];
				}
			}	
			
			
			context.write(new IntWritable(0), new MatrixMatrix(size, size, tmp));
	
		}
		
	}

		
	public static class MyReducer extends Reducer<IntWritable, MatrixMatrix, IntWritable, MatrixMatrix> {

		public void reduce(IntWritable key, Iterable<MatrixMatrix> values, Context context) throws IOException, InterruptedException 
		{	
			Double[][] result,tmp;
			
			
			Iterator<MatrixMatrix> iter = values.iterator();
			MatrixMatrix val;
			
			if(iter.hasNext())
			{
				val = iter.next();
				result = val.getValues();				
			}
			else throw new IOException("It shouldn't be never verified");
			
			int row = val.getRowNumber();
			int column = val.getColumnNumber();
			
			while (iter.hasNext()) 
			{
				val = iter.next();
				tmp = val.getValues();
				
				for(int i=0; i<row; i++)
					for(int j=0; j< column; j++)
						result[i][j] += tmp[i][j]; 
			}
			
			context.write(new IntWritable(0), new MatrixMatrix(row,column,result));
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();

		Job job = new Job(conf, "MapRed Step3");
		job.setJarByClass(HPhase3.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MatrixMatrix.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
