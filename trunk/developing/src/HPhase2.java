

import java.io.IOException;
import java.util.ArrayList;
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

/**
 *
 * @author virgilid
 */
public class HPhase2 {

	// OUTPUT VALUE MUST BE TEXT?
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

     
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {           	
        	String chunkName = ((FileSplit) context.getInputSplit()).getPath().getName();
        	if(chunkName.startsWith("W"))
        	{
        		int i, row;
        		for(i=0; i<chunkName.length() && 
        					  chunkName.charAt(i)>='0' && chunkName.charAt(i)<='9'; i++);      		
        		
        		try
        		{
        			String rowNumber = chunkName.substring(i);
        			row = new Integer(rowNumber);
        		}
        		catch(NumberFormatException e) { throw new IOException("Conversione nel nome del file fallita"); }
        	
        		//MatrixVector mv = new MatrixVector(value);
            	context.write(new IntWritable(row), new Text("W"+value.toString())); // TO BE MODIFIED

        	}
        	else if (chunkName.startsWith("A"))
        	{
        		SparseElement se = new SparseElement(value);
        		SparseVectorElement sve = new SparseVectorElement(se.getColumn(), se.getValue());
            	context.write(new IntWritable(se.getRow()), new Text("A"+sve.toString())); // TO BE MODIFIED

        	}
        	else throw new IOException("The file to be analyzed is not the correct one");
        }
       
    }

    public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, MatrixVector> {


		private void scalarProductEmit(Double[] dValues, SparseVectorElement tmp, Context context) throws IOException, InterruptedException 
		{

			if(tmp.getValue() != 0.0)
			{
				Double[] doubleTmp = new Double[dValues.length];
				
				for(int j=0; j<doubleTmp.length; j++) 
					doubleTmp[j] *= tmp.getValue();
				
				MatrixVector mvEmit = new MatrixVector(dValues.length, doubleTmp);
				
				context.write( new IntWritable(tmp.getCoordinate()), mvEmit);
			}
		}
    	
    	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
    		
    		ArrayList<SparseVectorElement> arrList = new ArrayList<SparseVectorElement>();
    		Iterator<Text> iter = values.iterator();
    		
    		Double[] dValues = null;
    		
    		Text val;
    		while(iter.hasNext())
    		{
    			val = iter.next();
    			
            	if(val.charAt(0) == 'A') /// MULTIPLE A ELEMENTS VALUE ARE NOT TAKEN IN ACCOUNT
            	{
            		SparseVectorElement sve = SparseVectorElement.parseLine(val.toString().substring(1));
            		arrList.add(sve);
            	}
            	else if(val.charAt(0) == 'W')
            	{
            		MatrixVector mv = MatrixVector.parseLine( val.toString().substring(1) );
            		dValues = mv.getValues();
            		
            		for(SparseVectorElement tmp: arrList)
            			scalarProductEmit(dValues, tmp, context);
            		
            		break;
            	}
    		}
    		
    		if(dValues != null)
    		{
    			while(iter.hasNext()) 
    			{
    				val = iter.next();
    			
    				if(val.charAt(0) == 'A') /// MULTIPLE A ELEMENTS VALUE ARE NOT TAKEN IN ACCOUNT
    				{
    					SparseVectorElement sve = SparseVectorElement.parseLine(val.toString().substring(1));
    					scalarProductEmit(dValues, sve, context);
    				}
    			}
    		}
        }


        
    }


    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //int totalsize = Integer.parseInt(args[2]);
        ///conf.setInt("matrix-mul-totalsize", totalsize); //the matrix is 'totalsize' by 'totalsize'

        Job job = new Job(conf, "MapRed Step1");
        job.setJarByClass(HPhase2.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //job.setNumReduceTasks(npart * npart);

        
//        job.setOutputKeyClass(SparseVectorElement.class);
//        job.setOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MatrixVector.class);

        //FileInputFormat.addInputPath(job, new Path(args[0]));
        TextInputFormat.addInputPath(job, new Path(args[0])); //need to read a complete line
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
    }
}
