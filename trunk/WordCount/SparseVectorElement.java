import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class SparseVectorElement implements WritableComparable<SparseVectorElement>{

		private Integer coordinate;
        private Double value;

        public SparseVectorElement(int coordinate, double value)
        {
            this.coordinate = coordinate;
            this.value = value;
        }
        
        public SparseVectorElement(Text s)
        {
        	parseLine(s.toString());
        }
        
        public SparseVectorElement()
        {
        	;
        }

        private void parseLine(String s)
        {
        	try
        	{
        		String[] splitted = s.split("#");
        		this.coordinate = new Integer(splitted[0]);
        		this.value = new Double(splitted[1]);
        	}
        	catch(NumberFormatException e) 
        	{
        		System.out.println("Input Error reading SparseElement Value" + s);
        		this.coordinate = 0;
        		this.value = 0.0;
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
		public void readFields(DataInput arg0) throws IOException 
		{
        	String tmp = arg0.readLine();
        	parseLine(tmp);
		}

		@Override
		public void write(DataOutput arg0) throws IOException 
		{
			arg0.writeBytes(this.toString());
		}

		@Override
		public int compareTo(SparseVectorElement o) 
		{
			if(this.coordinate - o.coordinate != 0)
				return this.coordinate - o.coordinate;
			
			return (int) (this.value - o.value);
		}
		
		public String toString()
		{
			return ""+ this.coordinate.toString() +"#" + this.value.toString() + "\n";
		}
}
