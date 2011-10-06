
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
public class HPhase1 {

	// OUTPUT VALUE MUST BE TEXT IN ORDER TO DISTINGUISH THE DIFFERENT DATA
	// TYPES?
	public static class MyMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String chunkName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();

			if (chunkName.startsWith("W")) /* A row vector must be emitted */
			{
				int i, row;
				for (i = 0; i < chunkName.length()
						&& (chunkName.charAt(i) < '0' || chunkName.charAt(i) > '9'); i++)
					;

				try {
					// the number present in the file name can be the number of
					// the first stored row vector
					// through a static variable we can take in account the
					// right row number knowing that the
					// row vector are read sequentially in the file split
					String rowNumber = chunkName.substring(i, i + 1);
					System.out.println("GUARDARE:" + rowNumber);
					row = new Integer(rowNumber);

				} catch (NumberFormatException e) {
					throw new IOException("File name conversion failled");
				}

				context.write(new IntWritable(row), new Text("W"
						+ value.toString())); // TO BE MODIFIED

			} else if (chunkName.startsWith("A")) /*
												 * The sparse element must be
												 * emitted
												 */
			{
				SparseElement se = new SparseElement(value);
				SparseVectorElement sve = new SparseVectorElement(se
						.getColumn(), se.getValue());
				context.write(new IntWritable(se.getRow()), new Text("A"
						+ sve.toString())); // TO BE MODIFIED

			} else
				throw new IOException(
						"The file to be analyzed is not a correct one");
		}

	}

	public static class MyReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		private void scalarProductEmit(Double[] dValues,
				SparseVectorElement tmp, Context context) throws IOException,
				InterruptedException {

			if (tmp.getValue() != 0.0) {
				Double[] doubleTmp = dValues.clone();
				for (int j = 0; j < doubleTmp.length; j++) {
					doubleTmp[j] *= tmp.getValue();
				}

				MatrixVector mvEmit = new MatrixVector(dValues.length,
						doubleTmp);

				context.write(new IntWritable(tmp.getCoordinate()), new Text(
						mvEmit.toString()));
			}
		}

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			/*
			 * the array contains the the row vector once the w row vector is
			 * readed
			 */
			Double[] dValues = null;

			/* The array list contains the sparse element on the key-th row */
			ArrayList<SparseVectorElement> arrList = new ArrayList<SparseVectorElement>();

			Text val;
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()) {
				val = iter.next();

				if (val.charAt(0) == 'A') // / MULTIPLE A ELEMENTS VALUE ARE NOT
											// TAKEN IN ACCOUNT
				{
					SparseVectorElement sve = SparseVectorElement.parseLine(val
							.toString().substring(1));
					arrList.add(sve);
				} else if (val.charAt(0) == 'W') {
					MatrixVector mv = MatrixVector.parseLine(val.toString()
							.substring(1));
					dValues = mv.getValues();

					System.out.println("Vettore aquisito");
					
					for (SparseVectorElement tmp : arrList)
						scalarProductEmit(dValues, tmp, context);

					/* Exit from the iterator loop */
					break;
				}
			}

			if (dValues == null)
				throw new IOException("Tne W's row vector is not received");			

			System.out.println("QUI CI ARRIVO");
			while (iter.hasNext()) {
				val = iter.next();

				if (val.charAt(0) == 'A') // / MULTIPLE A ELEMENTS VALUE ARE
					// NOT TAKEN IN ACCOUNT
				{
					SparseVectorElement sve = SparseVectorElement
					.parseLine(val.toString().substring(1));
					scalarProductEmit(dValues, sve, context);
				} else if (val.charAt(0) == 'W')
					throw new IOException("There is a double emission of the W's row vector");

			}
		}
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "MapRed Step1");
		job.setJarByClass(HPhase1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
