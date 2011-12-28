package HPhaseText;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.*;

public class HPhase4 {

    /* The output values must be text in order to distinguish the different data types */
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, NMFVector> {

        private static NMFMatrix WW;

        protected void setup(Context context) throws IOException {
            // MI PRENDO LA MATRICE DAL FILE ESTERNO
            Configuration conf = context.getConfiguration();
            NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));

            String otherFiles = conf.get("otherFiles", null);
            if (otherFiles != null) {
                FileSystem fs = FileSystem.get(conf);
                //creo il path dei file esterni
                Path inFile = new Path(otherFiles);
                FSDataInputStream in = fs.open(inFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

                String input;
                StringBuilder sb = new StringBuilder();
                input = br.readLine();
                while (!input.isEmpty()) {
                    sb.append(input);
                    input = br.readLine();
                }
                //System.out.println("DA FILE HO LETTO: " + sb.toString());

                // stampa di debug del file esterno, seccata perche non so come stampa uno string builder

                WW = NMFMatrix.parseLine(sb.toString()); //WW.parseLine(sb.toString());
                //System.out.println("QUESTA E LA MATRICE WW CHE HO PARSATO: " + WW.toString());
            }

        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int column = Integer.parseInt(value.toString().split("\t")[0]);
            String vector = value.toString().split("\t")[1];
            NMFVector mv = NMFVector.parseLine(vector);

            //System.out.println("MI ARRIVA STO VETTORE: " + mv.toString());
            NMFVector out = NMFMatrix.vectorMul(WW, mv);
            //System.out.println("HO FATTO LA MOLTIPLICAZIONE: " + out.toString());
            context.write(new IntWritable(column), out);

        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("The number of the input parameter are not corrected");
            System.err.println("First Parameter: H files directories");
            System.err.println("Second Parameter: HPhase3 output file");
            System.err.println("Third Parameter: Output directory");
            System.err.println("Fourth Parameter: K");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("elementsNumber", Integer.parseInt(args[3]));

        conf.set("otherFiles", args[1]);
        Job job = new Job(conf, "MapRed Step4");
        job.setJarByClass(HPhase4.class);
        job.setMapperClass(MyMapper.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NMFVector.class);

        job.setNumReduceTasks(0);

        // Testing Job Options


        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
