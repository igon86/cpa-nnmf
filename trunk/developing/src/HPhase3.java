import java.util.Iterator;
import java.io.IOException;

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


/**
 * 
 * @author virgilid
 */
public class HPhase3 {

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, MatrixMatrix> {

		protected void setup(Context context) throws IOException
		{
			String chunkName = ((FileSplit) context.getInputSplit()).getPath().getName();

			/* the number present in the file name is the number of the first stored row vector
			// Through a static variable we take in account the right row number knowing that the
			// row vector are read sequentially in the file split 
			*/
			
			if (!chunkName.startsWith("W"))
			{
				throw new IOException("File name is not correct");
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{

			MatrixVector mv = new MatrixVector(value);
			
			MatrixMatrix result = mv.externalProduct(mv);
			
			//System.out.println("External Prod = "+result);
						
			context.write(new IntWritable(1), result);
	
		}
		
	}

		
	public static class MyReducer extends Reducer<IntWritable, MatrixMatrix, IntWritable, MatrixMatrix> {

		public void reduce(IntWritable key, Iterable<MatrixMatrix> values, Context context) throws IOException, InterruptedException 
		{	
			MatrixMatrix result;
			
			Iterator<MatrixMatrix> iter = values.iterator();
			MatrixMatrix val;
			
			if(iter.hasNext())
			{
				result = iter.next();				
			}
			else throw new IOException("It shouldn't be never verified");
			
			
			while (iter.hasNext()) 
			{
				val = iter.next();
				result = result.sum(val);
			}
			
			context.write(new IntWritable(0), result);
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

		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MatrixMatrix.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		//TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
