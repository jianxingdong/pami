package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GreedyCSSMapper extends
		Mapper<LongWritable, Text, IntWritable, DoubleWritable[]> {

	private int numPartions;
	private int numColsPerPart;

	public GreedyCSSMapper(int numPartions, int numColumns) {
		this.numPartions = numPartions;
		this.numColsPerPart = numColumns / numPartions;
	}

	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, DoubleWritable[]>.Context context)
			throws java.io.IOException, InterruptedException {

		String sampleStr = value.toString();
		String[] ftrs = sampleStr.split("-");
		int offset = 0;
		int lastIndex = numPartions - 1;
		for (int i = 0; i < lastIndex; i++) {

			context.write(new IntWritable(i),
					getSamplePartion(ftrs, offset, numColsPerPart, false));
			offset += numColsPerPart;
		}
		context.write(new IntWritable(lastIndex),
				getSamplePartion(ftrs, offset, numColsPerPart, true));
	};

	private DoubleWritable[] getSamplePartion(String[] features, int offset,
			int size, boolean isLastPart) {
		if (isLastPart) {
			size = features.length - offset;
		}
		DoubleWritable[] part = new DoubleWritable[size];
		for (int i = 0; i < size; i++) {
			part[i] = new DoubleWritable(
					Double.parseDouble(features[i + offset]));

		}
		return part;

	}
}
