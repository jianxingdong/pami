package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;

public class Utils {

	public static DoubleArrayWritable getColumn(double[][] dataMatrix, int col) {
		int m = dataMatrix.length;
		DoubleWritable[] colArr = new DoubleWritable[m];
		for (int i = 0; i < m; i++) {
			colArr[i] = new DoubleWritable(dataMatrix[i][col]);
		}
		return new DoubleArrayWritable(colArr);

	}

	public static double[] toNativeDoubleArray(Writable[] hdbArray) {
		double[] nativeArray = new double[hdbArray.length];
		for (int i = 0; i < hdbArray.length; i++)
			nativeArray[i] = ((DoubleWritable) hdbArray[i]).get();
		return nativeArray;
	}

	public static double[] toNativeDoubleArray(Vector c) {
		double[] arr = new double[c.size()];
		Iterator<Vector.Element> itr = c.iterator();
		int i = 0;
		while (itr.hasNext())
			arr[i++] = itr.next().get();
		return arr;
	}
}
