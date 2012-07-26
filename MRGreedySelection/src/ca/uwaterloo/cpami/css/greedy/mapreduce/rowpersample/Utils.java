package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class Utils {

	public static ArrayWritable getColumn(double[][] dataMatrix, int col) {
		int m = dataMatrix.length;
		DoubleWritable[] colArr = new DoubleWritable[m];
		for (int i = 0; i < m; i++) {
			colArr[i] = new DoubleWritable(dataMatrix[i][col]);
		}
		return new ArrayWritable(DoubleWritable.class, colArr);

	}

	public static double[] toNativeDoubleArray(Writable[] hdbArray) {
		double[] nativeArray = new double[hdbArray.length];
		for (int i = 0; i < hdbArray.length; i++)
			nativeArray[i] = ((DoubleWritable) hdbArray[i]).get();
		return nativeArray;
	}
}
