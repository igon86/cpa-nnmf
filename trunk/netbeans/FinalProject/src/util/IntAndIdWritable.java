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

    private static String debug= System.getProperty("DEBUG", "false");

    public IntAndIdWritable()
    {
		super();
    }

    public IntAndIdWritable(int intValue, char charValue) {
	super(intValue);
	this.id = charValue;
    }

    public IntAndIdWritable(String intValue,char charValue){
	super(Integer.parseInt(intValue));
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
	System.out.println("IntAndIdWritable. Sono nella compareTo: "+this.toString() + " "+o.toString());
	int compare_value = super.compareTo(o);

	return (compare_value==0)? this.id - ((IntAndIdWritable)o).id : compare_value;
  }
  

	@Override
  public String toString()
  {
    return super.toString()+"-"+this.id;
  }


    /** A Comparator optimized for IntWritable. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(IntAndIdWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
	System.out.println("SONO nella compareBYTES: ");

      int thisValue = readInt(b1, s1);
      int thatValue = readInt(b2, s2);
      /**
      DataInputBuffer buffer = new DataInputBuffer();
      WritableComparable t1 = new IntAndIdWritable();
      WritableComparable t2 = new IntAndIdWritable();
      WritableComparable t3 = new Text();
      WritableComparable t4 = new Text();

      try {
                    buffer.reset(b1, s1, l1);
                    t1.readFields(buffer);
		    //buffer.reset(b1, s1+l1-1, 1);
		    //t3.readFields(buffer);
                    System.out.println("ARG1: "+t1.toString());
		    System.out.println("INT1:" +thisValue);
                    buffer.reset(b2, s2, l2);
                    t2.readFields(buffer);
		    //buffer.reset(b2, s2+21-1, 1);
		    //t4.readFields(buffer);
		    System.out.println("ARG2: "+t2.toString());
                    System.out.println("INT2: "+thatValue);

      } catch (IOException e) {
                    System.out.println("problem in debugging IntAndIdWritable compare bytes");
                    throw new RuntimeException(e);
      }
      */
      // QUI i char sono in UTF quindi occupano 2 bytes
      int confrontoChar = compareBytes(b1, s1+l1-2, 2 , b2, s2+l2 -2, 2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? confrontoChar : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(IntAndIdWritable.class, new Comparator());
  }
}
