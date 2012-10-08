package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

public class FinalSelectionMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
	private final IntWritable zeroWritable = new IntWritable(0);

	protected void map(
			IntWritable key,
			VectorWritable value,
			org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		context.write(zeroWritable, value);
	};

}
