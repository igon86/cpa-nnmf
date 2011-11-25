package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class NMFVector implements WritableComparable<NMFVector> {

    private static int elementsNumber = 0;
    protected double[] value;

    public static void setElementsNumber(int value){
	elementsNumber = value;
    }

    public NMFVector(int element_number, double[] elements) {
	//this.elementsNumber = element_number;
	this.value = elements;
    }

    public NMFVector(Text s) throws IOException {
	parseLine(s.toString(), this);
    }

    public NMFVector() {
	;
    }

    static public NMFVector parseLine(String s) throws IOException {
	NMFVector mv = new NMFVector();
	parseLine(s, mv);
	return mv;

    }
    // vector format : numbe_of_elements#elem1#elem2#elem3....
    // a vector (row or column) per line
    // no new line at the end of a file

    static private void parseLine(String s, NMFVector mv) throws IOException {

	if (elementsNumber == 0) throw new IOException("fail read fields");

	try {

		mv.value = new double[elementsNumber];

		String[] splitted = s.split("#");
	    System.out.println("stringa partizionata");

	    for (int i = 0; i < mv.elementsNumber && i <= splitted.length; i++) {
		mv.value[i] = new Double(splitted[i]);
		System.out.println("Ho acquisito il " + i + "-esimo parametro");
	    }
	    System.out.println("Valori del vettore presi");
	} catch (NumberFormatException e) {
	    System.out.println("Input Error reading SparseElement Value" + s);
	    mv.value = null;
	}
    }

    public int getNumberOfElement() {
	return this.elementsNumber;
    }

    public double[] getValues() {
	return this.value;
    }

    public NMFMatrix externalProduct(NMFVector v) // Tensor Product
    {
	int size = this.getNumberOfElement();
	if (this.getNumberOfElement() != v.getNumberOfElement()) {
	    return null;
	}

	double[][] tmp = new double[size][size];
	double[] vect1 = this.getValues();
	double[] vect2 = v.getValues();

	for (int i = 0; i < size; i++) {
	    for (int j = 0; j < size; j++) {
		tmp[i][j] = vect1[i] * vect2[j];
	    }
	}

	return new NMFMatrix(size, size, tmp);
    }

    public double internalProduct(NMFVector v) {
	if (this.elementsNumber != v.elementsNumber) {
	    return 0;
	}
	int ret = 0;
	for (int i = 0; i < this.elementsNumber; i++) {
	    ret += (this.value[i] * v.value[i]);
	}
	return ret;
    }

    public NMFVector ScalarProduct(double value) {
	double[] doubleTmp = this.value.clone();

	for (int i = 0; i < this.elementsNumber; i++) {
	    doubleTmp[i] = doubleTmp[i] * value;
	}

	return new NMFVector(this.getNumberOfElement(), doubleTmp);
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
	//this.elementsNumber = arg0.readInt();
	if (elementsNumber == 0) throw new IOException("fail read fields");
	this.value = new double[this.elementsNumber];
	for (int i = 0; i < this.elementsNumber; i++) {
	    this.value[i] = arg0.readDouble();
	}
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
	//arg0.writeInt(this.elementsNumber);
	for (int i = 0; i < this.elementsNumber; i++) {
	    arg0.writeDouble(this.value[i]);
	}
    }

    @Override
    public int compareTo(NMFVector o) {
	if (this.elementsNumber - o.elementsNumber != 0) {
	    return this.elementsNumber - o.elementsNumber;
	}
	/*
	int i;
	for(i=0; i<this.elementsNumber; i++)
	if(this.value[i] - o.value[i] != 0)
	return (int) (this.value[i] - o.value[i]);
	 */
	return 0;
    }

    public String toString() {
	System.out.println("SONO NELLA TOSTRING DI MATRIXVECTOR");

	String tmp = ""+this.value[0];
	StringBuilder stringBuilder = new StringBuilder(tmp);

	for (int i = 1; i < this.elementsNumber; i++) {
	    stringBuilder.append("#");
		stringBuilder.append(this.value[i]);
	}

	//stringBuilder.append('\n');
	return stringBuilder.toString();
    }

    public void inPlacePointDiv(NMFVector m) throws IOException {
	if (this.elementsNumber != m.elementsNumber) {
	    throw new IOException();
	}
	for (int i = 0; i < this.elementsNumber; i++) {

	    this.value[i] /= m.value[i];

	}
    }
       public void inPlacePointMul(NMFVector m) throws IOException {
	if (this.elementsNumber != m.elementsNumber) {
	    throw new IOException();
	}
	for (int i = 0; i < this.elementsNumber; i++) {

	    this.value[i] *= m.value[i];

	}
    }
       
       public void inPlaceSum(NMFVector m) throws IOException {
	   if (this.elementsNumber != m.elementsNumber) {
	    throw new IOException();
	}
	for (int i = 0; i < this.elementsNumber; i++) {

	    this.value[i] += m.value[i];

	}
       }

}
