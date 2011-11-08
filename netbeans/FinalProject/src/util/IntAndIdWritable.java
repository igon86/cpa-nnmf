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

    public IntAndIdWritable() {
	super();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	super.readFields(in);
	in.readChar(id);
    }

    @Override
    public void write(DataOutput out) throws IOException {
	super.write(out);
	out.writeChar(id);
	
    }

  /** Returns true iff <code>o</code> is a IntWritable with the same value. */
  public boolean equals(Object o) {
    if (!(o instanceof IntWritable))
      return false;
    IntWritable other = (IntWritable)o;
    return this.value == other.value;
  }

  public int hashCode() {
    return value;
  }

  /** Compares two IntWritables. */
  public int compareTo(Object o) {
    int thisValue = this.value;
    int thatValue = ((IntWritable)o).value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }

  public String toString() {
    return Integer.toString(value);
  }
}
