package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import ca.uwaterloo.cpami.css.greedy.core.GreedyColSubsetSelection;

/**
 * 
 * The output file is the new matrix C
 * 
 */
public class FinalSelectionReducer extends
		Reducer<IntWritable, SelectedColumn, IntWritable, VectorWritable> {

	private int k;

	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<IntWritable, SelectedColumn, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		this.k = context.getConfiguration().getInt("subsetSize", 0);
	};

	protected void reduce(
			IntWritable key,
			java.lang.Iterable<SelectedColumn> columns,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, SelectedColumn, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {

		// building the data matrix
		List<Vector> columnsList = new ArrayList<Vector>();
		List<Integer> indices = new ArrayList<Integer>();
		Iterator<SelectedColumn> colsItr = columns.iterator();
		while (colsItr.hasNext()) {
			SelectedColumn sc = colsItr.next();
			indices.add(sc.getColumnIndex().get());
			columnsList.add(sc.getColumn().get());
		}

		int m = columnsList.get(0).size();
		Array2DRowRealMatrix dataMatrix = new Array2DRowRealMatrix(m,
				columnsList.size());


		int i = 0;
		for (Vector c : columnsList)
			dataMatrix.setColumn(i++, Utils.toNativeDoubleArray(c));

		// apply local Greedy subset selection
		// the partition-based approach is not used for now
		Integer[] selectedColumns = new GreedyColSubsetSelection()
				.selectColumnSubset(dataMatrix, dataMatrix, k);
		// writing the selected indices & columns

		Array2DRowRealMatrix selectedColsMatrix = new Array2DRowRealMatrix(m, k);

		i = 0;
		for (Integer col : selectedColumns) {
			selectedColsMatrix.setColumn(i++, dataMatrix.getColumn(col));
		}

		for (int j = 0; j < m; j++)
			context.write(new IntWritable(j), new VectorWritable(
					new DenseVector(selectedColsMatrix.getRow(j))));
	};

}
