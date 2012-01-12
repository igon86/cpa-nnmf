package HPhaseText;

import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.*;

public class HPhase3 {

    /* The output values must be text in order to distinguish the different data types */
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, NMFMatrix> {

        @Override
        protected void setup(Context context) throws IOException {
            NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String vector = value.toString().split("\t")[1];

            NMFVector mv = NMFVector.parseLine(vector);

            NMFMatrix result = mv.externalProduct(mv);

            //System.out.println("External Prod = " + result.toString());

            context.write(new IntWritable(0), result);

        }
    }

    /**
     * null writable is used in order to serialize a MatrixMatrix only
     */
    public static class MyReducer extends Reducer<IntWritable, NMFMatrix, NullWritable, NMFMatrix> {

		@Override
        protected void setup(Context context) {
            NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));

        }

        @Override
        public void reduce(IntWritable key, Iterable<NMFMatrix> values, Context context) throws IOException, InterruptedException {
            NMFMatrix result;

            Iterator<NMFMatrix> iter = values.iterator();
            NMFMatrix val;

            if (iter.hasNext()) {
                val = iter.next();
                result = new NMFMatrix(val.getRowNumber(), val.getColumnNumber(), val.getValues().clone());
                //System.out.println("REDUCE: ho ricevuto: " + result.toString());
            } else {
                throw new IOException("No vectors to sum");
            }


            while (iter.hasNext()) {
                val = iter.next();
                //System.out.println("REDUCE: ho ricevuto: " + val.toString());
                if (!result.inPlaceSum(val)) {
                    System.err.println("ERRORE nella somma di matrici");
                    throw new IOException("ERRORE nella somma di matrici");
                }
            }

            context.write(NullWritable.get(), result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("The number of the input parameter are not corrected");
            System.err.println("First Parameter: W files directories");
            System.err.println("Second Parameter: Output directory");
            System.err.println("Third Parameter: K ");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("elementsNumber", Integer.parseInt(args[2]));


        Job job = new Job(conf, "MapRed Step3");
        job.setJarByClass(HPhase3.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Testing Job Options
        //job.setNumReduceTasks(0);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NMFMatrix.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
