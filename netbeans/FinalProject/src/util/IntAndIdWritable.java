/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author andrealottarini
 */
public class IntAndIdWritable extends IntWritable {
    private char id;

    public IntAndIdWritable()
    {
		super();
    }

    public IntAndIdWritable(int intValue, char charValue) {
	super(intValue);
	this.id = charValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
		super.readFields(in);
		this.id=in.readChar();
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
		super.write(out);
		out.writeChar(id);
	
    }

    public char getId(){
	return this.id;
    }

  /** Returns true iff <code>o</code> is a IntWritable with the same value. */
	@Override
  public boolean equals(Object o)
  {
    if (!(o instanceof IntWritable))
      return false;
    IntWritable otherObject = (IntWritable)o;
    return this.get() == otherObject.get();
  }


   //Compares two IntWritables.
	@Override
  public int compareTo(Object o)
  {
	int compare_value = super.compareTo(o);

	return (compare_value==0)? this.id - ((IntAndIdWritable)o).id : compare_value;
  }
  

	@Override
  public String toString()
  {
    return super.toString()+"-"+this.id;
  }
    // eredità il comparatore da intWritable: in questo modo riesce a fare il grouping
	// funziona perche' l'int writable e' stato scritto per primo
    //static{
    //	    WritableComparator.define(IntAndIdWritable.class, new IntWritable.Comparator());
    //}
}
