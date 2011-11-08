
package HPhase;


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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.*;

public class HPhase4 {

    /* The output values must be text in order to distinguish the different data types */
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, MatrixVector> {

        private static MatrixMatrix WW;
        private static int currentColumn;

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
                FSDataInputStream in = fs.open(inFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

				String input;
                StringBuilder sb = new StringBuilder();
                input = br.readLine();
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

            // GUARDO DOVE SONO NELLA MATRICE H
            String chunkName = ((FileSplit) context.getInputSplit()).getPath().getName();
            int i, j;

            for (i = 0; i < chunkName.length()
                    && (chunkName.charAt(i) < '0' || chunkName.charAt(i) > '9'); i++);

            for (j = i; j < chunkName.length()
                    && (chunkName.charAt(j) >= '0' && chunkName.charAt(j) <= '9'); j++);

            try
			{
                String columnNumber = chunkName.substring(i, j);
                System.out.println("LA PRIMA COLONNA E:" + columnNumber);
                currentColumn = new Integer(columnNumber);
            }
			catch (NumberFormatException e)
			{
                throw new IOException("File name conversion failled");
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{

			String vector = value.toString().split("\t")[1];
			MatrixVector mv = MatrixVector.parseLine(vector);

			System.out.println("MI ARRIVA STO VETTORE: "+mv.toString());
			MatrixVector out = MatrixMatrix.vectorMul(WW,mv);
			System.out.println("HO FATTO LA MOLTIPLICAZIONE: "+out.toString());
            context.write(new IntWritable(currentColumn++),out);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("otherFiles", args[2]);
        Job job = new Job(conf, "MapRed Step4");
        job.setJarByClass(HPhase4.class);
        job.setMapperClass(MyMapper.class);

	job.setNumReduceTasks(0);
	
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MatrixVector.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
