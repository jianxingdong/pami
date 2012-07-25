package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import ca.uwaterloo.cpami.css.greedy.core.GreedyColSubsetSelection;

public class PartitionSelectionReducer extends
		Reducer<IntWritable, SamplePartition, IntWritable, IntWritable> {

	private int k;

	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<IntWritable, SamplePartition, IntWritable, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		this.k = context.getConfiguration().getInt("partitionSubsetSize", 0);
	};

	protected void reduce(
			IntWritable key,
			java.lang.Iterable<SamplePartition> partition,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, SamplePartition, IntWritable, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {

		// building the matrix
		ArrayList<double[]> samples = new ArrayList<double[]>();
		Iterator<SamplePartition> itr = partition.iterator();
		Writable[] indices = null;
		while (itr.hasNext()) {
			SamplePartition sp = itr.next();
			if (sp.isIndices()) {
				indices = sp.getColIndices();
			} else {
				samples.add(toNativeDoubleArray(sp.getSamplePart()));
			}
		}

		double[][] dataMatrix = new double[samples.size()][];
		int i = 0;
		for (double[] sample : samples)
			dataMatrix[i++] = sample;

		// apply local Greedy subset selection
		// the partition-based approach is not used for now
		Integer[] selectedColumns = new GreedyColSubsetSelection()
				.selectColumnSubset(dataMatrix, dataMatrix, k);
		// writing the selected indices & columns
		// TODO
		i = 0;
		for (Integer col : selectedColumns) {
			context.write(new IntWritable(++i), new IntWritable(col));
		}
	};

	private double[] toNativeDoubleArray(Writable[] hdbArray) {
		double[] nativeArray = new double[hdbArray.length];
		for (int i = 0; i < hdbArray.length; i++)
			nativeArray[i] = ((DoubleWritable) hdbArray[i]).get();
		return nativeArray;
	}
}
