package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class NMFMatrix implements WritableComparable<NMFMatrix> {

    private int rowNumber;
    private int columnNumber;
    private double[][] value;

    public NMFMatrix(int row_number, int column_number, double[][] elements) {
	this.rowNumber = row_number;
	this.columnNumber = column_number;
	this.value = elements;
    }

    public NMFMatrix(Text s) {
	parseLine(s.toString(), this);
    }

    public NMFMatrix() {
	;
    }

    static public NMFMatrix parseLine(String s) {
	NMFMatrix mv = new NMFMatrix();
	parseLine(s, mv);
	return mv;

    }
    // vector format : numbe_of_elements#elem1#elem2#elem3....
    // a vector (row or column) per line
    // no new line at the end of a file

    static private void parseLine(String s, NMFMatrix mv) {

	try {
	    String[] splitted = s.split("\t");
	    //System.out.println("MatrixMatrix: stringa partizionata");

	    String[] tmp = splitted[0].split("#");
	    //System.out.println("parsing a #");

	    //System.out.println("Conversione 1:<" + tmp[0] + ">");
	    mv.rowNumber = Integer.parseInt(tmp[0]);

	    //System.out.println("Conversione 2:<" + tmp[1] + ">");
	    mv.columnNumber = Integer.parseInt(tmp[1]);

	    mv.value = new double[mv.rowNumber][mv.columnNumber];
	    //System.out.println("Dimensione presa " + splitted.length);

	    for (int row = 1; row < splitted.length && row - 1 < mv.rowNumber; row++) {
		//System.out.println("SPLITTED ROW " + row + " " + splitted[row]);
		tmp = splitted[row].split("#");
		//System.out.println("row partitioned in " + tmp.length + " values");
		for (int i = 0; i < mv.columnNumber && i <= splitted.length; i++) {
		    //System.out.println("row " + i + "^ value = <" + tmp[i] + ">");
		    mv.value[row - 1][i] = new Double(tmp[i]);
		    //System.out.println("Ho acquisito il " + i + "-esimo parametro");
		}
		//System.out.println("MATRIXMATRIX: PARSELINE TERMINATA CON SUCCESSO");
	    }

	} catch (NumberFormatException e) {
	    System.out.println("Input Error reading MatrixMatrix Value <" + s + ">");
	    System.out.println(e.toString());
	    mv.columnNumber = 0;
	    mv.rowNumber = 0;
	    mv.value = null;
	}

    }

    public int getRowNumber() {
	return this.rowNumber;
    }

    public int getColumnNumber() {
	return this.columnNumber;
    }

    public double[][] getValues() {
	return this.value;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
	this.rowNumber = arg0.readInt();
	this.columnNumber = arg0.readInt();
	double[][] values = new double[this.rowNumber][this.columnNumber];
	for (int i = 0; i < this.rowNumber; i++) {
	    for (int j = 0; j < this.columnNumber; j++) {
		values[i][j] = arg0.readDouble();
	    }
	}
	this.value = values;
    }

    @Override
    public void write(DataOutput arg0) throws IOException {

	//System.out.println("Sono nella write obj" + this.toString());
	arg0.writeInt(this.rowNumber);
	arg0.writeInt(this.columnNumber);
	for (int i = 0; i < this.rowNumber; i++) {
	    for (int j = 0; j < this.columnNumber; j++) {
		arg0.writeDouble(this.value[i][j]);
	    }
	}
    }

    @Override
    public int compareTo(NMFMatrix o) {
	if (this.rowNumber - o.rowNumber != 0) {
	    return this.rowNumber - o.rowNumber;
	}

	if (this.columnNumber - o.columnNumber != 0) {
	    return this.rowNumber - o.rowNumber;
	}

	/*
	for(int i=0; i<this.rowNumber; i++)
	for(int j=0; j<this.columnNumber; j++)
	if(this.value[i][j] - o.value[i][j] != 0)
	return (int) (this.value[i][j] - o.value[i][j]);
	 */
	return 0;
    }

    public String toString() {
	String tmp = "" + this.rowNumber;
	StringBuilder stringBuilder = new StringBuilder(tmp);

	stringBuilder.append("#" + this.columnNumber);

	stringBuilder.append('\t');


	for (int i = 0; i < this.rowNumber; i++) {
	    stringBuilder.append(this.value[i][0]);
	    for (int j = 1; j < this.columnNumber; j++) {
		stringBuilder.append("#" + this.value[i][j]);
	    }

	    if (i < this.rowNumber - 1) {
		stringBuilder.append('\t');
	    }
	}
	stringBuilder.append('\n');

	//System.out.println("Sono nella printf " + stringBuilder.toString());

	return stringBuilder.toString();
    }

    /** Implements sum of matrices
     *
     * @param m
     *      Matrix which has to be summed with this
     * @return
     *      false if dimensions of this and m do not agree
     *      true if the sum was computed correctly
     */
    public boolean inPlaceSum(NMFMatrix m) {
	if (this.rowNumber != m.rowNumber || this.columnNumber != m.columnNumber) {
	    return false;
	}
	//System.out.println("Sto per sommare " + this.toString() + "CON " + m.toString());
	//System.out.println("Sto per sommare " + this.value + "CON " + m.value);
	for (int i = 0; i < this.rowNumber; i++) {
	    for (int j = 0; j < this.columnNumber; j++) {
		//System.out.println("sommo this.value[" +i+"]["+j+"]: ");
		//System.out.println(this.value[i][j]);
		//System.out.println(m.value[i][j]);
		this.value[i][j] += m.value[i][j];
	    }
	}
	return true;
    }

    public static NMFVector vectorMul(NMFMatrix m, NMFVector v) {
	NMFVector ret = new NMFVector(m.rowNumber, new double[m.rowNumber]);
	for (int i = 0; i < m.rowNumber; i++) {
	    ret.value[i] = m.getRowVector(i).internalProduct(v);
	}
	return ret;
    }

    public static NMFVector leftVectorMul(NMFMatrix m ,NMFVector v){
	NMFVector ret = new NMFVector(m.rowNumber, new double[m.rowNumber]);
	for (int i = 0; i < m.rowNumber; i++) {
	    ret.value[i] = m.getColumnVector(i).internalProduct(v);
	}
	return ret;
    }

    private NMFVector getColumnVector(int i){
	double[] out = new double[this.columnNumber];
	for (int j = 0; j < out.length;j++){
	    out[i] = this.value[j][i];
	}
	NMFVector mv = new NMFVector(out.length, out);
	//System.out.println("il vettore "+i+"di C e: "+mv.toString());
	return mv;
    }

    private NMFVector getRowVector(int i) {

	return new NMFVector(this.value[i].length, this.value[i]);
    }



    public void inPlacePointMul(NMFMatrix m) throws IOException {
	if (this.rowNumber != m.rowNumber || this.columnNumber != m.columnNumber) {
	    throw new IOException();
	}
	for (int i = 0; i < this.rowNumber; i++) {
	    for (int j = 0; j < this.columnNumber; j++) {
		this.value[i][j] *= m.value[i][j];
	    }
	}
    }

    public void inPlacePointDiv(NMFMatrix m) throws IOException {
	if (this.rowNumber != m.rowNumber || this.columnNumber != m.columnNumber) {
	    throw new IOException();
	}
	for (int i = 0; i < this.rowNumber; i++) {
	    for (int j = 0; j < this.columnNumber; j++) {
		this.value[i][j] /= m.value[i][j];
	    }
	}
    }
}
