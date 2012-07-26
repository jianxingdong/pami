package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalSelectionMapper extends
		Mapper<IntWritable, ArrayWritable, IntWritable, SelectedColumn> {

	protected void map(
			IntWritable key,
			ArrayWritable value,
			org.apache.hadoop.mapreduce.Mapper<IntWritable, ArrayWritable, IntWritable, SelectedColumn>.Context context)
			throws java.io.IOException, InterruptedException {
		context.write(new IntWritable(0), new SelectedColumn(key, value));
	};

}
