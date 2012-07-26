package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import ca.uwaterloo.cpami.css.greedy.core.GreedyColSubsetSelection;

public class FinalSelectionReducer extends
		Reducer<IntWritable, SelectedColumn, IntWritable, ArrayWritable> {

	private int k;

	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<IntWritable, SelectedColumn, IntWritable, ArrayWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		this.k = context.getConfiguration().getInt("subsetSize", 0);
	};

	protected void reduce(
			IntWritable key,
			java.lang.Iterable<SelectedColumn> columns,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, SelectedColumn, IntWritable, ArrayWritable>.Context context)
			throws java.io.IOException, InterruptedException {

		// building the data matrix
		List<ArrayWritable> columnsList = new ArrayList<ArrayWritable>();
		List<Integer> indices = new ArrayList<Integer>();
		Iterator<SelectedColumn> colsItr = columns.iterator();
		while (colsItr.hasNext()) {
			SelectedColumn sc = colsItr.next();
			indices.add(sc.getColumnIndex().get());
			columnsList.add(sc.getColumn());
		}

		RealMatrix dataMatrix = new Array2DRowRealMatrix(columnsList.get(0)
				.get().length, columnsList.size());

		int i = 0;
		for (ArrayWritable c : columnsList)
			dataMatrix.setColumn(i++, Utils.toNativeDoubleArray(c.get()));

		// apply local Greedy subset selection
		// the partition-based approach is not used for now
		double[][] matrix = dataMatrix.getData();
		Integer[] selectedColumns = new GreedyColSubsetSelection()
				.selectColumnSubset(matrix, matrix, k);
		// writing the selected indices & columns
		for (Integer col : selectedColumns) {
			context.write(new IntWritable(indices.get(col)),
					Utils.getColumn(matrix, col));
		}
	};
}
