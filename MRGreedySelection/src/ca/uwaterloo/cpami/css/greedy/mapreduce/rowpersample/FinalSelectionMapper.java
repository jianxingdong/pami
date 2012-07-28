package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalSelectionMapper extends
		Mapper<IntWritable, DoubleArrayWritable, IntWritable, SelectedColumn> {

	protected void map(
			IntWritable key,
			DoubleArrayWritable value,
			org.apache.hadoop.mapreduce.Mapper<IntWritable, DoubleArrayWritable, IntWritable, SelectedColumn>.Context context)
			throws java.io.IOException, InterruptedException {
		context.write(new IntWritable(0), new SelectedColumn(key, value));
	};

}
