package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class PartitionSelectionMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, SamplePartition> {

	private int numPartitions;
	private int numColsPerPart;

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, SamplePartition>.Context context)
			throws java.io.IOException, InterruptedException {
		this.numPartitions = context.getConfiguration().getInt("numPartitions",
				0);
		this.numColsPerPart = context.getConfiguration()
				.getInt("numColumns", 0) / this.numPartitions;

	};

	private boolean isFirstPair = true;

	protected void map(
			IntWritable key,
			VectorWritable value,
			org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, SamplePartition>.Context context)
			throws java.io.IOException, InterruptedException {

		int offset = 0;
		int lastIndex = numPartitions - 1;
		boolean isFirstMap = isFirstPair
				&& context.getTaskAttemptID().getId() == 0;
		for (int i = 0; i < lastIndex; i++) {

			context.write(
					new IntWritable(i),
					getSamplePartition(value.get(), offset, numColsPerPart,
							false));

			// only the first mapper sends the indices of the columns of each
			// partition
			if (isFirstMap) {
				context.write(
						new IntWritable(i),
						getColIndices(offset, numColsPerPart, false, value
								.get().size()));
			}
			offset += numColsPerPart;
		}
		context.write(new IntWritable(lastIndex),
				getSamplePartition(value.get(), offset, numColsPerPart, true));
		if (isFirstMap) {
			context.write(
					new IntWritable(lastIndex),
					getColIndices(offset, numColsPerPart, true, value.get()
							.size()));
		}
		isFirstPair = false;
	};

	private SamplePartition getSamplePartition(Vector features, int offset,
			int size, boolean isLastPart) {
		if (isLastPart) {
			size = features.size() - offset;
		}
		DoubleWritable[] part = new DoubleWritable[size];
		for (int i = 0; i < size; i++) {
			part[i] = new DoubleWritable(features.getQuick(i));

		}
		SamplePartition sp = new SamplePartition();
		sp.setIndices(false);
		sp.setSamplePart(part);
		return sp;
	}

	private SamplePartition getColIndices(int offset, int partSize,
			boolean isLastPart, int featureSize) {
		if (isLastPart) {
			partSize = featureSize - offset;
		}
		IntWritable[] indices = new IntWritable[partSize];
		for (int i = 0; i < partSize; i++) {
			indices[i] = new IntWritable(offset + i);
		}
		SamplePartition sp = new SamplePartition();
		sp.setIndices(true);
		sp.setColIndices(indices);
		return sp;
	}
}