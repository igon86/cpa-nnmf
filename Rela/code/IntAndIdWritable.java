
public class IntAndIdWritable extends IntWritable {
    private char id;
	
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
	
	/** Compares two IntWritables. */
	@Override
	public int compareTo(Object o)
	{
		int compare_value = super.compareTo(o);
		
		return (compare_value==0)? this.id - ((IntAndIdWritable)o).id : compare_value;
	}
	
    /** A Comparator optimized for IntAndIdWritable. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(IntAndIdWritable.class);
		}
		
		public int compare(byte[] b1, int s1, int l1,
						   byte[] b2, int s2, int l2) {
			
			int thisValue = readInt(b1, s1);
			int thatValue = readInt(b2, s2);
			
			/** char are in UTF -> 2 byte */
			int confrontoChar = compareBytes(b1, s1+l1-2, 2 , b2, s2+l2 -2, 2);
			return (thisValue<thatValue ? -1 : (thisValue==thatValue ? confrontoChar : 1));
		}
	}
	
	static {                                        // register this comparator
		WritableComparator.define(IntAndIdWritable.class, new Comparator());
	}
}
