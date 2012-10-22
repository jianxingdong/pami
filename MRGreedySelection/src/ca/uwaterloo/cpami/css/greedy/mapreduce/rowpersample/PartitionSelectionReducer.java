package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import ca.uwaterloo.cpami.css.greedy.core.LocalGreedyCSS;

public class PartitionSelectionReducer extends
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
			java.lang.Iterable<VectorWritable> partition,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		// building the matrix
		Iterator<VectorWritable> itr = partition.iterator();
		if (!itr.hasNext()) {
			return;
		}
		Vector firstRow = itr.next().get();
		// transpose of the original matrix partition
		SparseMatrix mx = new SparseMatrix(firstRow.size(), numRows);
		mx.assignColumn(0, firstRow);
		int rowIndx = 1;
		while (itr.hasNext()) {
			mx.assignColumn(rowIndx++, itr.next().get());
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
		// writing the k selected columns (output matrix is of size kxnumRows)
		rowIndx = 0;
		for (int col : selectedColumns) {
			rowIndxWritable.set(rowIndx++);
			rowWritable.set(mx.viewRow(col));
			context.write(rowIndxWritable, rowWritable);
		}
	}
}