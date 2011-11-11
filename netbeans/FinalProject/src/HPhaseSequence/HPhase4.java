
package HPhaseSequence;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.*;

public class HPhase4 {

    /* The output values must be text in order to distinguish the different data types */
    public static class MyMapper extends Mapper<IntWritable, MatrixVector, IntWritable, MatrixVector> {

        private static MatrixMatrix WW;

        protected void setup(Context context) throws IOException
		{
            // MI PRENDO LA MATRICE DAL FILE ESTERNO
            Configuration conf = context.getConfiguration();
            String otherFiles = conf.get("otherFiles", null);
            if (otherFiles != null)
	    {
                FileSystem fs = FileSystem.get(conf);
                //creo il path dei file esterni
                Path inFile = new Path(otherFiles);
		SequenceFile
                FSDataInputStream in = fs.open(inFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String input;
                StringBuilder sb = new StringBuilder();
                input = br.read
		while (!input.isEmpty())
				{
                    sb.append(input);
                    input = br.readLine();
                }
		System.out.println("DA FILE HO LETTO: "+sb.toString());
		
                // stampa di debug del file esterno, seccata perche non so come stampa uno string builder

                WW = MatrixMatrix.parseLine(sb.toString()); //WW.parseLine(sb.toString());
		System.out.println("QUESTA E LA MATRICE WW CHE HO LETTO: "+WW.toString());
            }

        }

        public void map(IntWritable key, MatrixVector value, Context context) throws IOException, InterruptedException
		{

			System.out.println("MI ARRIVA STO VETTORE: "+value.toString());
			MatrixVector out = MatrixMatrix.vectorMul(WW,value);
			System.out.println("HO FATTO LA MOLTIPLICAZIONE: "+out.toString());
			context.write(key,out);

        }
    }

    public static void main(String[] args) throws Exception 
	{
		if(args.length != 3)
		{
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First Parameter: H files directories");
			System.err.println("Second Parameter: HPhase3 output file");
			System.err.println("Third Parameter: Output directory");
			System.exit(-1);
		}

        Configuration conf = new Configuration();
        conf.set("otherFiles", args[1]);
        Job job = new Job(conf, "MapRed Step4");
        job.setJarByClass(HPhase4.class);
        job.setMapperClass(MyMapper.class);
	
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MatrixVector.class);

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	job.setNumReduceTasks(0);

		// Testing Job Options


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
