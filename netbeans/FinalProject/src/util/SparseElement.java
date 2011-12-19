
package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class SparseElement implements WritableComparable<SparseElement>{

	private Integer rowCoordinate;
        private Integer columnCoordinate;
        private Double value;

        public SparseElement(int x, int y, double value)
        {
            this.rowCoordinate = x;
            this.columnCoordinate = y;
            this.value = value;
        }

        public SparseElement(Text s)
        {
        	parseLine(s.toString(), this);
        }

        public SparseElement()
        {
        	;
        }

        static public SparseElement parseLine(String s)
        {
        	SparseElement se = new SparseElement();
        	parseLine(s, se);
        	return se;
        }

        static private void parseLine(String s, SparseElement se)
        {
                String[] splitted = s.split("#");
        	try
        	{
        		se.rowCoordinate = new Integer(splitted[0]);
        		
        		
        	}
        	catch(NumberFormatException e)
        	{
        		System.out.println("Error parseLine ROW of SparseElement:" + s+"\nROW: "+splitted[0]);
        		se.rowCoordinate = 0;
        	}
                
                try
        	{
        		se.columnCoordinate = new Integer(splitted[1]);
        	}
        	catch(NumberFormatException e)
        	{
        		System.out.println("Error parseLine COLUMN of SparseElement:" + s+"\nCOLUMN: "+splitted[1]);

        		se.columnCoordinate = 0;

        	}

                try
        	{

        		se.value = new Double(splitted[2]);
        	}
        	catch(NumberFormatException e)
        	{
        		System.out.println("Error parseLine VALUE of SparseElement:" + s+"\nVALUE: "+splitted[2]);
                        System.out.print("VALUE: ");
                        for (int i=0; i< splitted[2].length();i++){
                            System.out.print(splitted[2].charAt(i)+"("+(int)splitted[2].charAt(i)+")");
                        }
                        System.out.println("FINITO");
        		se.value = 0.0;
        	}
        }

        public int getRow()
        {
            return this.rowCoordinate;
        }

        public int getColumn()
        {
            return this.columnCoordinate;
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
		public int compareTo(SparseElement o)
		{
			if(this.rowCoordinate - o.rowCoordinate != 0)
				return this.rowCoordinate - o.rowCoordinate;

			if(this.columnCoordinate - o.columnCoordinate != 0)
				return this.columnCoordinate - o.columnCoordinate;

			return (int) (this.value - o.value);
		}

		public String toString()
		{
			return ""+ this.rowCoordinate.toString() +"#" +
					this.columnCoordinate.toString()+"#" + this.value.toString();
		}
}

