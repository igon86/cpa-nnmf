/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author andrealottarini
 */
public class GenericElement extends GenericWritable{

    private static Class[] CLASSES = {
	SparseVectorElement.class,
	NMFVector.class,
    };

    @Override
    protected Class[] getTypes() {
	return CLASSES;
    }

}
