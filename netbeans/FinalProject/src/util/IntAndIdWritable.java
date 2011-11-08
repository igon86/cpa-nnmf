/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

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

  /** Returns true iff <code>o</code> is a IntWritable with the same value. */
	@Override
  public boolean equals(Object o)
  {
    if (!(o instanceof IntWritable))
      return false;
    IntWritable otherObject = (IntWritable)o;
    return this.get() == otherObject.get();
  }


  /** Compares two IntWritables. */
	@Override
  public int compareTo(Object o)
  {
	int compare_value = super.compareTo(o);

	return (compare_value==0)? 0 : this.id - ((IntAndIdWritable)o).id;
  }

	@Override
  public String toString()
  {
    return super.toString()+"-"+this.id;
  }
}
