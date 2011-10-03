import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class MatrixVector implements WritableComparable<MatrixVector>{

		private int elementsNumber;
        private Double[] value;

        public MatrixVector(int element_number)
        {
            this.elementsNumber = element_number;
            this.value = new Double[element_number];
        }
        
        public MatrixVector(Text s)
        {
        	parseLine(s.toString());
        }
        
        public MatrixVector()
        {
        	;
        }

        // vector format : numbe_of_elements#elem1#elem2#elem3....
        // a vector (row or column) per line
        // no new line at the end of a file
        private void parseLine(String s)
        {
        	try
        	{
        		String[] splitted = s.split("#");
        		System.out.println("stringa partizionata");
        		
        		this.elementsNumber = new Integer(splitted[0]);
        		this.value = new Double[this.elementsNumber];
        		System.out.println("Dimensione presa");
        		
        		for(int i=0; i<elementsNumber && i<=splitted.length; i++)
        		{
        			this.value[i] = new Double(splitted[i+1]);
        			System.out.println("Ho acquisito il "+ i +"-esimo parametro");
        		}
        		System.out.println("Valori del vettore presi");
        	}
        	catch(NumberFormatException e) 
        	{
        		System.out.println("Input Error reading SparseElement Value" + s);
        		this.elementsNumber = 0;
        		this.value = null;
        	}
        }
        
        public int getNumberOfElement()
        {
            return this.elementsNumber;
        }

        public Double[] getValues()
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
		public int compareTo(MatrixVector o) 
		{
			if(this.elementsNumber - o.elementsNumber != 0)
				return this.elementsNumber - o.elementsNumber;
			
			int i;
			for(i=0; i<this.elementsNumber; i++)
				if(this.value[i] - o.value[i] != 0)
					return (int) (this.value[i] - o.value[i]);
			
			return 0;
		}
		
		public String toString()
		{
			String tmp = ""+ this.elementsNumber;
			StringBuilder stringBuilder = new StringBuilder(tmp);
						
			for(int i=0; i<this.elementsNumber; i++)
				stringBuilder.append("#" + this.value[i]);
						
			stringBuilder.append('\n');
			return stringBuilder.toString();
		}
}
