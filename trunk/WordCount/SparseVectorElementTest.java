/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author virgilid
 */
public class SparseVectorElementTest {

    public static class MyMapper extends Mapper<LongWritable, Text, SparseVectorElement, IntWritable> {

     
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {           	
        	System.out.println("FINISCI???");
        	SparseVectorElement se = new SparseVectorElement(value);
        	System.out.println(se);
        	System.out.println("ORA VEDIAMO");
        	context.write(se, new IntWritable(1));
        	System.out.println("SI ALLA FINE CE L'ho FATTA!!!");
        }
       
    }

    public static class MyReducer extends Reducer<SparseVectorElement, IntWritable, SparseVectorElement, IntWritable> {


    	public void reduce(SparseVectorElement key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
        	int counter = 0;
        	System.out.println(key);
            for (IntWritable val : values) 
            {
            	counter += val.get();
            }
            context.write( key, new IntWritable(counter));
        }
        
    }


    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //int totalsize = Integer.parseInt(args[2]);
        ///conf.setInt("matrix-mul-totalsize", totalsize); //the matrix is 'totalsize' by 'totalsize'

        Job job = new Job(conf, "provaTipoVettoreSparso");
        job.setJarByClass(SparseVectorElementTest.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //job.setNumReduceTasks(npart * npart);

        
//        job.setOutputKeyClass(SparseVectorElement.class);
//        job.setOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(SparseVectorElement.class);
        job.setOutputValueClass(IntWritable.class);

        //FileInputFormat.addInputPath(job, new Path(args[0]));
        TextInputFormat.addInputPath(job, new Path(args[0])); //need to read a complete line
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
    }
}
