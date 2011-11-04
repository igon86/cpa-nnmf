
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MatrixMatrix implements WritableComparable<MatrixMatrix> {

    private int rowNumber;
    private int columnNumber;
    private Double[][] value;

    public MatrixMatrix(int row_number, int column_number, Double[][] elements) {
        this.rowNumber = row_number;
        this.columnNumber = column_number;
        this.value = elements;
    }

    public MatrixMatrix(Text s) {
        parseLine(s.toString(), this);
    }

    public MatrixMatrix() {
        ;
    }

    static public MatrixMatrix parseLine(String s) {
        MatrixMatrix mv = new MatrixMatrix();
        parseLine(s, mv);
        return mv;

    }
    // vector format : numbe_of_elements#elem1#elem2#elem3....
    // a vector (row or column) per line
    // no new line at the end of a file

    static private void parseLine(String s, MatrixMatrix mv) {

        try {
            String[] splitted = s.split("\t");
            System.out.println("MatrixMatrix: stringa partizionata");

            String[] tmp = splitted[0].split("#");
            mv.rowNumber = new Integer(tmp[0]);
            mv.columnNumber = new Integer(tmp[1]);

            mv.value = new Double[mv.rowNumber][mv.columnNumber];
            System.out.println("Dimensione presa " + splitted.length);

            for (int row = 1; row < splitted.length && row - 1 < mv.rowNumber; row++) {
                System.out.println("SPLITTED ROW " + row + " " + splitted[row]);
                tmp = splitted[row].split("#");
                System.out.println("row partitioned in " + tmp.length + " values");
                for (int i = 0; i < mv.columnNumber && i <= splitted.length; i++) {
                    System.out.println("row " + i + "^ value = <" + tmp[i] + ">");
                    mv.value[row - 1][i] = new Double(tmp[i]);
                    System.out.println("Ho acquisito il " + i + "-esimo parametro");
                }
                System.out.println("Valori del vettore presi");
            }
        } catch (NumberFormatException e) {
            System.out.println("Input Error reading MatrixMatrix Value" + s);
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

    public Double[][] getValues() {
        return this.value;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        String tmp = arg0.readLine();
        parseLine(tmp);
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeBytes(this.toString());
    }

    @Override
    public int compareTo(MatrixMatrix o) {
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
        return stringBuilder.toString();
    }

    /** Implements sum of matrices
     *
     * @param m
     *      Matrix which has to be summed with this
     * @return
     *      null if dimensions of this and m do not agree
     *      MatrixMatrix of the sum otherwise
     */
    public MatrixMatrix sum(MatrixMatrix m){
        if(this.rowNumber != m.rowNumber || this.columnNumber !=  m.columnNumber) return null;
        MatrixMatrix ret = new MatrixMatrix(this.rowNumber, this.columnNumber, value);
        for(int i = 0 ;i<this.rowNumber;i++){
            for (int j = 0; i<this.columnNumber;j++){
                ret.value[i][j] += m.value[i][j];
            }
        }
        return ret;
    }
}
