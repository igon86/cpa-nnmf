public class Phase1 {

    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private static boolean W = false;
        private static int currentRow = 0;

        @Override
        protected void setup(Context context) throws IOException {
            String chunkName = ((FileSplit) context.getInputSplit()).getPath().getName();
            if (chunkName.startsWith("W")) {
                W = true;
            } else if (!chunkName.startsWith("A")) {
                throw new IOException("File name not correct");
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /** The vector is stored one element per line in the form row#value
             *	The matrix is stored one element per line in the form column#row%value,  */
			String[] values = value.toString().split("#");
            if (V) {
                context.write( new IntWritable(Integer.parseInt(values[0])), new Text("V" + values[1] ) );
            } else /* The sparse element must be emitted */ {
                IntWritable column = new IntWritable(Integer.parseInt(values[0]));
                context.write(column, new Text("a" + values[1]));
            }
        }
    }

    public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<Text> matrixValues = new ArrayList<Text>();
            Text val;
            NMFVector vectorValue = 0, outputValue;

            Iterator<Text> iter = values.iterator();

            while (iter.hasNext()) {
                val = iter.next();
                if (val.charAt(0) == 'a') {
                    matrixValues.add(val);
                } else {
                    vectorValue = NMFVector.parseLine(val.toString().substring(1));
                }
            }
            /* elements are emitted by scanning the list of received elements from the matrix */
            iter = matrixValues.iterator();
            while (iter.hasNext()) {
                val = iter.next();
                String[] rowValue = val.toString().split("%");
                outputValue = vectorValue.multiply(Double.parseDouble(rowValue[1]));
                context.write(new IntWritable(Integer.parseInt(rowValue[0])), new Text("" + outputValue.toString));
            }
        }
    }
}
