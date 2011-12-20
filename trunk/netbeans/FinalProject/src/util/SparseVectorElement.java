package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class SparseVectorElement implements WritableComparable<SparseVectorElement>{

	private int coordinate;
        private double value;

        public SparseVectorElement(int coordinate, double value)
        {
            this.coordinate = coordinate;
            this.value = value;
        }

        public SparseVectorElement(Text s) throws IOException
        {
        	parseLine(s.toString(), this);
        }

        public SparseVectorElement()
        {
        	;
        }

        public void set(int coordinate, double value)
        {
            this.coordinate = coordinate;
            this.value = value;
        }

        static public SparseVectorElement parseLine(String s) throws IOException
        {
        	SparseVectorElement sve = new SparseVectorElement();
        	parseLine(s, sve);
        	return sve;
        }

        static private void parseLine(String s, SparseVectorElement sve) throws IOException
        {
        	try
        	{
        		String[] splitted = s.split("#");
        		sve.coordinate = new Integer(splitted[0]);
        		sve.value = new Double(splitted[1]);
        	}
        	catch(NumberFormatException e)
        	{
        		System.err.println("Input Error reading SparseVectorElement Value" + s);
        		sve.coordinate = 0;
        		sve.value = 0.0;
                        throw new IOException();
        	}
        }

        public int getCoordinate()
        {
            return this.coordinate;
        }

        public Double getValue()
        {
        	return this.value;
        }

        @Override
		public void readFields(DataInput din) throws IOException
		{
			this.coordinate = din.readInt();
			this.value = din.readDouble();
		}

		@Override
		public void write(DataOutput dout) throws IOException
		{
			dout.writeInt(coordinate);
			dout.writeDouble(value);
		}

		@Override
		public int compareTo(SparseVectorElement o)
		{
			if(this.coordinate - o.coordinate != 0)
				return this.coordinate - o.coordinate;

			return (int) (this.value - o.value);
		}

	@Override
		public String toString()
		{
			return ""+ this.coordinate +"#" + this.value;
		}
}
