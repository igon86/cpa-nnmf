
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class TestTypes {

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
                        System.out.println(value.toString() + " " + Integer.parseInt(value.toString()) );
			context.write(key, new IntWritable(Integer.parseInt(value.toString())));
			
		}
//lower case is usefull for the ordering of the key

	}

	public static class MyReducer extends Reducer<LongWritable, IntWritable, Text, Text> {

                
		public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
                    
                    for(IntWritable i:values){
                            context.write(new Text(key.toString()), new Text(i.toString()));
                    }
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		Job job = new Job(conf,"Test Types");
		job.setJarByClass(TestTypes.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

                //these two are mandatory
                job.setMapOutputKeyClass(LongWritable.class);
                job.setMapOutputValueClass(IntWritable.class);
                //these two can be anything -____-
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
