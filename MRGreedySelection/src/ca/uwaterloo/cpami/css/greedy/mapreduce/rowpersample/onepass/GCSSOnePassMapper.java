package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample.onepass;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.VectorWritable;

import ca.uwaterloo.cpami.css.greedy.core.LocalGreedyCSS;
import ca.uwaterloo.cpami.css.greedy.core.ProgressNotifiable;

//partition-based selection
//output the matrix of the selected rows
public class GCSSOnePassMapper extends
		Mapper<NullWritable, MatrixWritable, NullWritable, VectorWritable> implements ProgressNotifiable{

	private int k;
	private Context context;
	@Override
	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<NullWritable, MatrixWritable, NullWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		this.k = context.getConfiguration().getInt("splitSubsetSize", -1);
		System.out.println("k mapper: " + k);
	};

	@Override
	protected void map(
			NullWritable key,
			MatrixWritable value,
			org.apache.hadoop.mapreduce.Mapper<NullWritable, MatrixWritable, NullWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		this.context = context;
		long time = System.currentTimeMillis();
		// select from the rows
		Matrix mx = value.get();
		System.out.println("size of mx  at mapper: " + mx.numRows() + ","
				+ mx.numCols());

		Runtime.getRuntime().gc();
		Runtime.getRuntime().gc();
		System.out.println("memory of mx at mapper: "
				+ (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
						.freeMemory()));

		Integer[] selectedColumns = new LocalGreedyCSS(this).selectColumnSubset(mx,
				k);
		for (int c : selectedColumns) {
			rowWritable.set(mx.viewRow(c));
			context.write(NullWritable.get(), rowWritable);
		}
		System.out.println("all mapper time: "
				+ (System.currentTimeMillis() - time));
	};

	private final VectorWritable rowWritable = new VectorWritable();

	@Override
	public void progress() {
		context.progress();
	}
}
