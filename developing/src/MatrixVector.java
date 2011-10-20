import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class MatrixVector implements WritableComparable<MatrixVector>{

	private int elementsNumber;
        private Double[] value;

        public MatrixVector(int element_number, Double[] elements)
        {
            this.elementsNumber = element_number;
            this.value = elements;
        }
        
        public MatrixVector(Text s)
        {
        	parseLine(s.toString(), this);
        }
        
        public MatrixVector()
        {
        	;
        }

        static public MatrixVector parseLine(String s)
        {
        	MatrixVector mv = new MatrixVector();
        	parseLine(s, mv);
        	return mv;
        	
        }
        // vector format : numbe_of_elements#elem1#elem2#elem3....
        // a vector (row or column) per line
        // no new line at the end of a file
        
        static private void parseLine(String s, MatrixVector mv)
        {
        	
        	try
        	{
        		String[] splitted = s.split("#");
        		System.out.println("stringa partizionata");
        		
        		mv.elementsNumber = new Integer(splitted[0]);
        		mv.value = new Double[mv.elementsNumber];
        		System.out.println("Dimensione presa");
        		
        		for(int i=0; i<mv.elementsNumber && i<=splitted.length; i++)
        		{
        			mv.value[i] = new Double(splitted[i+1]);
        			System.out.println("Ho acquisito il "+ i +"-esimo parametro");
        		}
        		System.out.println("Valori del vettore presi");
        	}
        	catch(NumberFormatException e) 
        	{
        		System.out.println("Input Error reading SparseElement Value" + s);
        		mv.elementsNumber = 0;
        		mv.value = null;
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
