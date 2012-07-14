package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import ca.uwaterloo.cpami.css.greedy.core.GreedyColSubsetSelection;

public class GreedyCSSReducer extends
		Reducer<IntWritable, DoubleWritable[], IntWritable, IntWritable> {

	private int k;

	public GreedyCSSReducer(int k) {
		this.k = k;
	}

	protected void reduce(
			IntWritable key,
			java.lang.Iterable<DoubleWritable[]> partition,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, DoubleWritable[], IntWritable, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {

		// building the matrix
		ArrayList<double[]> samples = new ArrayList<double[]>();
		Iterator<DoubleWritable[]> itr = partition.iterator();
		while (itr.hasNext()) {
			samples.add(toNativeDoubleArray(itr.next()));
		}

		double[][] dataMatrix = new double[samples.size()][];
		int i = 0;
		for (double[] sample : samples)
			dataMatrix[i++] = sample;

		// apply local Greedy subset selection
		// the partition-based approach is not used for now
		Integer[] selectedColumns = new GreedyColSubsetSelection()
				.selectColumnSubset(dataMatrix, dataMatrix, k);
		// writing the selected indices
		i = 0;
		for (Integer col : selectedColumns) {
			context.write(new IntWritable(++i), new IntWritable(col));
		}
	};

	private double[] toNativeDoubleArray(DoubleWritable[] hdbArray) {
		double[] nativeArray = new double[hdbArray.length];
		for (int i = 0; i < hdbArray.length; i++)
			nativeArray[i] = hdbArray[i].get();
		return nativeArray;
	}
}
