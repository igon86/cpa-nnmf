package HPhaseSequence;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import util.*;

public class HPhase4 {

	/* The output values must be text in order to distinguish the different data types */
	public static class MyMapper extends Mapper<IntWritable, GenericElement, IntWritable, GenericElement> {

		private static NMFMatrix WW = new NMFMatrix();

		@Override
		protected void setup(Context context) throws IOException {
			NMFVector.setElementsNumber(context.getConfiguration().getInt("elementsNumber", 0));

			// MI PRENDO LA MATRICE DAL FILE ESTERNO
			Configuration conf = context.getConfiguration();
			String otherFiles = conf.get("otherFiles", null);
			if (otherFiles != null) {
				FileSystem fs = FileSystem.get(conf);

				Path inFile = new Path(otherFiles);

				FileStatus fileStatus = fs.getFileStatus(inFile);
				if (!fileStatus.isDir()) {
					throw new IOException("The file isn't a directory");
				}

				FileStatus[] list = fs.listStatus(inFile);
				Path cMatrix = null;
				for (int i = 0; i < list.length; i++) {
					cMatrix = list[i].getPath();
					if (fs.getFileStatus(cMatrix).isDir()) {
						cMatrix = null;
					} else {
						break;
					}
				}

				if (cMatrix == null) {
					throw new IOException("The directory doesn't contain the data file");
				}

				//creo il path dei file esterni
				//        Path inFile = new Path(otherFiles);
				SequenceFile.Reader sfr = new SequenceFile.Reader(fs, cMatrix, conf);
				sfr.next(NullWritable.get(), WW);
				/*
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
				//System.out.println("DA FILE HO LETTO: "+sb.toString());

				// stampa di debug del file esterno, seccata perche non so come stampa uno string builder

				WW = MatrixMatrix.parseLine(sb.toString()); //WW.parseLine(sb.toString());
				 **/
				//System.out.println("QUESTA E LA MATRICE WW CHE HO LETTO: "+WW.toString());

			}

		}

		@Override
		public void map(IntWritable key, GenericElement value, Context context) throws IOException, InterruptedException {
			NMFVector mv = (NMFVector) value.get();
			//System.out.println("MI ARRIVA STO VETTORE: "+mv.toString());
			NMFVector out = NMFMatrix.vectorMul(WW, mv);
			//System.out.println("HO FATTO LA MOLTIPLICAZIONE: "+out.toString());
			GenericElement gw = new GenericElement();
			gw.set(out);
			context.write(key, gw);

		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("The number of the input parameter are not corrected");
			System.err.println("First Parameter: H files directories");
			System.err.println("Second Parameter: HPhase3 output file");
			System.err.println("Third Parameter: Output directory");
			System.err.println("Fourth Parameter: The factorizing parameter of the NNMF (K)");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.setInt("elementsNumber", Integer.parseInt(args[3]));
		conf.set("otherFiles", args[1]);
		Job job = new Job(conf, "MapRed Step4");
		job.setJarByClass(HPhase4.class);
		job.setMapperClass(MyMapper.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(GenericElement.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}
}
