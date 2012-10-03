package ca.uwaterloo.cpami.mahout.matrix.utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

public class FrobeniusNormJob {

	public static class FrobNormMapper extends
			Mapper<IntWritable, VectorWritable, IntWritable, DoubleWritable> {
		
		final DoubleWritable norm = new DoubleWritable();
		final IntWritable zero = new IntWritable(0);
		protected void map(
				IntWritable key,
				VectorWritable value,
				org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, DoubleWritable>.Context context)
				throws java.io.IOException, InterruptedException {
			norm.set(Math.pow(value.get().norm(2), 2));
			context.write(zero, norm);
		};
	}
	
	
}
