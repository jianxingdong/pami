package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class PartitionSelectionMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	private int numPartitions;
	private int numColsPerPart;
	private int pMinusOne;

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		this.numPartitions = context.getConfiguration().getInt("numPartitions",
				0);
		this.numColsPerPart = context.getConfiguration()
				.getInt("numColumns", 0) / this.numPartitions;
		pMinusOne = numPartitions - 1;
	};

	private VectorWritable partWritable = new VectorWritable();
	private IntWritable partNumberWritable = new IntWritable();

	protected void map(
			IntWritable key,
			VectorWritable value,
			org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {

		int offset = 0;
		for (int part = 0; part < pMinusOne; part++) {
			partNumberWritable.set(part);
			partWritable.set(getPart(value.get(), offset, numColsPerPart));
			context.write(partNumberWritable, partWritable);
			offset += numColsPerPart;
		}
		partNumberWritable.set(pMinusOne);
		partWritable.set(getPart(value.get(), offset, value.get().size()
				- offset));
		context.write(partNumberWritable, partWritable);
	};

	private double tmpVal;

	private RandomAccessSparseVector getPart(Vector v, int offset, int length) {
		RandomAccessSparseVector partialVector = new RandomAccessSparseVector(
				length);
		for (int i = 0; i < length; i++) {
			if ((tmpVal = v.getQuick(offset + i)) != 0)
				partialVector.set(i, tmpVal);
		}
		return partialVector;
	}

}