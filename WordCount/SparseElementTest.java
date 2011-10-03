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
public class SparseElementTest {

    public static class MyMapper extends Mapper<LongWritable, Text, SparseElement, IntWritable> {

       /* @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //get filename
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filename = fileSplit.getPath().getName();
            if ("left".equalsIgnoreCase(filename)) {
                isLeftMatrix = true;
            } else {
                isLeftMatrix = false;
            }

            //get how size and partition information
            Configuration conf = context.getConfiguration();
            totalSize = conf.getInt("matrix-mul-totalsize", -1);
            partSize = conf.getInt("matrix-mul-partsize", -1);
            npart = conf.getInt("mTextInputFormatatrix-mul-npart", -1);
            if (totalSize < 0 || partSize < 0 || npart < 0) {
                System.out.println("Error in setup of MyMapper.");
                System.exit(1);
            }
        }
        */
        
       /* @Override
        public void map(LongWritable key, SparseElement value, Context context) throws IOException, InterruptedException {
            IntWritable reducerId;
            Text outputValue = new Text();
            String[] array = value.toString().split("[#,]");
            int i = Integer.parseInt(array[0]);
            int j = Integer.parseInt(array[1]);
            int elementValue = Integer.parseInt(array[2]);
            int row = i / partSize;
            int column = j / partSize;
            if (isLeft()) {
                outputValue.set("l#" + Integer.toString(i) + "#" + Integer.toString(j) + "#" + Integer.toString(elementValue));
                for (int k = 0; k < partSize; k++) {
                    Pair out = new Pair(row, k);
                    context.write(out, outputValue);
                }
            } else {
                outputValue.set("r#" + Integer.toString(i) + "#" + Integer.toString(j) + "#" + Integer.toString(elementValue));
                for (int k = 0; k < partSize; k++) {
                    Pair out = new Pair(k, column);
                    context.write(out, outputValue);
                }
            }


        } */
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {           	
        	SparseElement se = new SparseElement(value);
        	System.out.println(se);
        	context.write(se, new IntWritable(1));
        }
        
    }

    public static class MyReducer extends Reducer<SparseElement, IntWritable, DoubleWritable, IntWritable> {

        public void reduce(SparseElement key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
        	int counter = 0;
        	System.out.println(key);
            for (IntWritable val : values) 
            {
            	counter += val.get();
            }
            context.write( new DoubleWritable(key.getValue()), new IntWritable(counter));
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //int totalsize = Integer.parseInt(args[2]);
        ///conf.setInt("matrix-mul-totalsize", totalsize); //the matrix is 'totalsize' by 'totalsize'

        Job job = new Job(conf, "provaTipoSparso");
        job.setJarByClass(SparseElementTest.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //job.setNumReduceTasks(npart * npart);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //FileInputFormat.addInputPath(job, new Path(args[0]));
        TextInputFormat.addInputPath(job, new Path(args[0])); //need to read a complete line
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
    }
}
