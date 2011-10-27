
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author virgilid
 */
public class TestPoly {

    public static class IntPair implements WritableComparable<IntPair> {

        protected int first = 0;
        protected int second = 0;

        /**
         * Set the left and right values.
         */
        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        /**
         * Read the two integers.
         * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
         */
        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readInt() + Integer.MIN_VALUE;
            second = in.readInt() + Integer.MIN_VALUE;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(first - Integer.MIN_VALUE);
            out.writeInt(second - Integer.MIN_VALUE);
        }

        @Override
        public int hashCode() {
            return first * 157 + second;
        }

        @Override
        public boolean equals(Object right) {
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }

        /** A Comparator that compares serialized IntPair. 
        public static class Comparator extends WritableComparator {

            public Comparator() {
                super(IntPair.class);
            }

            public int compare(byte[] b1, int s1, int l1,
                    byte[] b2, int s2, int l2) {
                return compareBytes(b1, s1, l1, b2, s2, l2);
            }
        }

        static {                                        // register this comparator
            WritableComparator.define(IntPair.class, new Comparator());
        }
         */

        @Override
        public int compareTo(IntPair o) {
            if (first != o.first) {
                return first < o.first ? -1 : 1;
            } else if (second != o.second) {
                return second < o.second ? -1 : 1;
            } else {
                return 0;
            }
        }
    }

    public static class IntTriple extends IntPair {

        protected int third = 0;

        /**
         * Set the left and right values.
         */
        public void set(int a, int b, int c) {
            first = a;
            second = b;
            third = c;
        }

        public int getThird() {
            return third;
        }

        /**
         * Read the two integers.
         * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
         */
        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readInt() + Integer.MIN_VALUE;
            second = in.readInt() + Integer.MIN_VALUE;
            third = in.readInt() + Integer.MIN_VALUE;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(first - Integer.MIN_VALUE);
            out.writeInt(second - Integer.MIN_VALUE);
            out.writeInt(third - Integer.MIN_VALUE);
        }

        @Override
        public int hashCode() {
            return first * 157 + second*23 + third;
        }

        @Override
        public boolean equals(Object right) {
            if (right instanceof IntTriple) {
                IntTriple r = (IntTriple) right;
                return r.first == first && r.second == second && r.third == third;
            } else {
                return false;
            }
        }

        /** A Comparator that compares serialized IntPair. 
        public static class Comparator extends WritableComparator {

            public Comparator() {
                super(IntPair.class);
            }

            public int compare(byte[] b1, int s1, int l1,
                    byte[] b2, int s2, int l2) {
                return compareBytes(b1, s1, l1, b2, s2, l2);
            }
        }

        static {                                        // register this comparator
            WritableComparator.define(IntPair.class, new Comparator());
        } */

        public int compareTo(IntTriple o) {
            if (first != o.first) {
                return first < o.first ? -1 : 1;
            } else if (second != o.second) {
                return second < o.second ? -1 : 1;
            } else if (third != o.third){
                return third < o.third ? -1 : 1;
            }else{
                return 0;
            }
        }
    }

    /* The output values must be text in order to distinguish the different data types */
    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, IntArrayWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int red = Integer.parseInt(value.toString());
            IntTriple ret = new IntTriple();
            ret.set(red, red, red);
            IntWritable[] a = new IntWritable[100];
            for (int i =0;i<a.length;i++){
                a[i] = new IntWritable(red+i);
            }
            IntArrayWritable aw = new IntArrayWritable();
            aw.set(a);
            context.write(key, aw);

        }
//lower case is usefull for the ordering of the key
    }

    public static class MyReducer extends Reducer<LongWritable, IntArrayWritable, Text, IntWritable> {

        public void reduce(LongWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {

            for (IntArrayWritable i : values) {

                Writable[] valori = i.get();
                int sum=0;
                for (int j =0;j<valori.length;j++){
                    IntWritable x = (IntWritable)valori[j];
                    sum+=x.get();
                }

                context.write(new Text(key.toString()),new IntWritable(sum));
            }
        }
    }

    /**
     * @param args
     *            the command line arguments
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Test Poly");
        job.setJarByClass(TestPoly.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(0);
        //these two are mandatory
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntArrayWritable.class);
        //these two can be anything -____-
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
