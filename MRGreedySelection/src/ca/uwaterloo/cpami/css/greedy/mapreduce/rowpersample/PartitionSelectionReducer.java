package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

import ca.uwaterloo.cpami.css.greedy.core.GreedyColSubsetSelection;

public class PartitionSelectionReducer extends
		Reducer<IntWritable, SamplePartition, IntWritable, VectorWritable> {

	private int k;

	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<IntWritable, SamplePartition, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		this.k = context.getConfiguration().getInt("partitionSubsetSize", 0);
	};

	protected void reduce(
			IntWritable key,
			java.lang.Iterable<SamplePartition> partition,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, SamplePartition, IntWritable, VectorWritable>.Context context)
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
				samples.add(Utils.toNativeDoubleArray(sp.getSamplePart()));
			}
		}

		double[][] dataMatrix = new double[samples.size()][indices.length];		
		int i = 0;
		for (double[] sample : samples)
			dataMatrix[i++] = sample;

		// apply local Greedy subset selection
		// the partition-based approach is not used for now

		Array2DRowRealMatrix mat = new Array2DRowRealMatrix(dataMatrix);
		Integer[] selectedColumns = new GreedyColSubsetSelection()
				.selectColumnSubset(mat, mat, k);
		// writing the selected indices & columns
		for (Integer col : selectedColumns) {
			context.write((IntWritable) indices[col], new VectorWritable(
					new DenseVector(mat.getColumn(col))));
		}
	}

}
