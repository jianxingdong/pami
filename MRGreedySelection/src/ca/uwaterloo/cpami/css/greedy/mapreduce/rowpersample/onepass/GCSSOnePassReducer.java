package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample.onepass;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.VectorWritable;

import ca.uwaterloo.cpami.css.greedy.core.LocalGreedyCSS;
import ca.uwaterloo.cpami.css.greedy.core.ProgressNotifiable;

//aggregate selection
//output the selected columns in mahout matrix format
public class GCSSOnePassReducer extends
		Reducer<NullWritable, VectorWritable, IntWritable, VectorWritable>
		implements ProgressNotifiable {

	private int k;

	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<NullWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		this.k = context.getConfiguration().getInt("subsetSize", 0);
	};

	private Context context;

	protected void reduce(
			NullWritable key,
			java.lang.Iterable<VectorWritable> columns,
			org.apache.hadoop.mapreduce.Reducer<NullWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		this.context = context;
		long time = System.currentTimeMillis();
		// building the matrix
		Iterator<VectorWritable> itr = columns.iterator();
		Map<Integer, RandomAccessSparseVector> rowMap = new HashMap<Integer, RandomAccessSparseVector>();
		Integer numRows = 0, numCols;
		while (itr.hasNext()) {
			rowMap.put(numRows++, (RandomAccessSparseVector) itr.next().get());
		}
		numCols = (numRows == 0) ? 0 : rowMap.get(0).size();
		SparseMatrix mx = new SparseMatrix(numRows, numCols, rowMap);

		System.out.println("size of mx  at reducer: " + mx.numRows() + ","
				+ mx.numCols());

		Runtime.getRuntime().gc();
		Runtime.getRuntime().gc();
		System.out.println("memory of mx at reducer: "
				+ (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
						.freeMemory()));
		// apply local Greedy subset selection
		Integer[] selectedColumns = new LocalGreedyCSS(this)
				.selectColumnSubset(mx, k);
		int i = 0;
		for (int c : selectedColumns) {
			rowWritable.set(mx.viewRow(c));
			rowNumWritable.set(i++);
			context.write(rowNumWritable, rowWritable);
		}

		System.out.println("all reduce time: "
				+ (System.currentTimeMillis() - time));
	};

	private final VectorWritable rowWritable = new VectorWritable();
	private final IntWritable rowNumWritable = new IntWritable();

	@Override
	public void progress() {
		context.progress();
	}
}
