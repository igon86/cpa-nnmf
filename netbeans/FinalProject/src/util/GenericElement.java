package util;

import org.apache.hadoop.io.GenericWritable;

public class GenericElement extends GenericWritable {

	private static Class[] CLASSES = {
		SparseVectorElement.class,
		NMFVector.class,};

	@Override
	protected Class[] getTypes() {
		return CLASSES;
	}
}
