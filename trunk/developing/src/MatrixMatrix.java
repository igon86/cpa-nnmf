import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class MatrixMatrix implements WritableComparable<MatrixMatrix>{

		private int rowNumber;
		private int columnNumber;
        private Double[][] value;

        public MatrixMatrix(int row_number, int column_number, Double[][] elements)
        {
            this.rowNumber = row_number;
            this.columnNumber = column_number;
            this.value = elements;
        }
        
        public MatrixMatrix(Text s)
        {
        	parseLine(s.toString(), this);
        }
        
        public MatrixMatrix()
        {
        	;
        }

        static public MatrixMatrix parseLine(String s)
        {
        	MatrixMatrix mv = new MatrixMatrix();
        	parseLine(s, mv);
        	return mv;
        	
        }
        // vector format : numbe_of_elements#elem1#elem2#elem3....
        // a vector (row or column) per line
        // no new line at the end of a file
        
        static private void parseLine(String s, MatrixMatrix mv)
        {
        	
        	try
        	{
        		String[] splitted = s.split("\n");
        		System.out.println("stringa partizionata");
        		
        		String[] tmp = splitted[0].split("#");
        		mv.rowNumber = new Integer(tmp[0]);
        		mv.columnNumber = new Integer(tmp[1]);
        		
        		mv.value = new Double[mv.rowNumber][mv.columnNumber];
        		System.out.println("Dimensione presa");
        		
        		for(int row=1; row<splitted.length; row++)
        		{
        			tmp = splitted[row].split("#");
        			for(int i=0; i<mv.columnNumber && i<=splitted.length; i++)
        			{
        				mv.value[row-1][i] = new Double(splitted[i]);
        				System.out.println("Ho acquisito il "+ i +"-esimo parametro");
        			}
        		System.out.println("Valori del vettore presi");
        		}
        	}
        	catch(NumberFormatException e) 
        	{
        		System.out.println("Input Error reading SparseElement Value" + s);
        		mv.columnNumber = 0;
        		mv.rowNumber = 0;
        		mv.value = null;
        	}
        }

		public int getRowNumber()
        {
            return this.rowNumber;
        }

		public int getColumnNumber()
        {
            return this.columnNumber;
        }
		
        public Double[][] getValues()
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
		public int compareTo(MatrixMatrix o) 
		{
			if(this.rowNumber - o.rowNumber != 0)
				return this.rowNumber - o.rowNumber;
			
			if(this.columnNumber - o.columnNumber != 0)
				return this.rowNumber - o.rowNumber;
			
			/*
			for(int i=0; i<this.rowNumber; i++)
				for(int j=0; j<this.columnNumber; j++)
					if(this.value[i][j] - o.value[i][j] != 0)
						return (int) (this.value[i][j] - o.value[i][j]);
			*/
			return 0;
		}
		
		public String toString()
		{
			String tmp = ""+ this.rowNumber;
			StringBuilder stringBuilder = new StringBuilder(tmp);
						
			stringBuilder.append("#" + this.columnNumber);

			stringBuilder.append('\n');
			for(int i=0; i<this.rowNumber; i++)
			{
				stringBuilder.append(this.value[i][0]);
				for(int j=1;j<this.columnNumber;j++)
				{
					stringBuilder.append("#" + this.value[i][j]);
				}
						
				stringBuilder.append('\n');
			}
			return stringBuilder.toString();
		}
}
