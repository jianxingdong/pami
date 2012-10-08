package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import ca.uwaterloo.cpami.css.greedy.core.LocalGreedyCSS;

public class FinalSelectionReducer extends
		Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	private int k;
	private int numRows;

	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		this.k = context.getConfiguration().getInt("subsetSize", 0);
		this.numRows = context.getConfiguration().getInt("numRows", 0);
	};

	private IntWritable rowIndxWritable = new IntWritable();
	private VectorWritable rowWritable = new VectorWritable();

	protected void reduce(
			IntWritable key,
			java.lang.Iterable<VectorWritable> columns,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		// building the matrix
		Iterator<VectorWritable> itr = columns.iterator();
		List<Vector> allCols = new ArrayList<Vector>();

		while (itr.hasNext()) {
			allCols.add(itr.next().get());
		}

		SparseMatrix mx = new SparseMatrix(allCols.size(), numRows);
		int rowIndx = 0;
		for (Vector v : allCols) {
			mx.assignRow(rowIndx++, v);
		}
		System.out.println("size of mx transpose: " + mx.numRows() + ","
				+ mx.numCols());
		// apply local Greedy subset selection
		// the partition-based approach is not used for now

		// no random partitioning here
		Integer[] selectedColumns = new LocalGreedyCSS().selectColumnSubset(mx,
				k);
		// selectedcols.length might be < k
		k = selectedColumns.length;
		// output matrix is of size numRows x k

		for (rowIndx = 0; rowIndx < numRows; rowIndx++) {
			rowIndxWritable.set(rowIndx);
			rowWritable
					.set(getRowPart(mx.viewColumn(rowIndx), selectedColumns));
			context.write(rowIndxWritable, rowWritable);
		}
	}

	private double tmpVal;

	private Vector getRowPart(Vector fullRow, Integer[] selectedColumns) {
		RandomAccessSparseVector rowPart = new RandomAccessSparseVector(k);
		int i = 0;
		for (int colIndx : selectedColumns) {
			if ((tmpVal = fullRow.getQuick(colIndx)) != 0) {
				rowPart.set(i, tmpVal);
			}
			i++;
		}
		return rowPart;
	}

}
