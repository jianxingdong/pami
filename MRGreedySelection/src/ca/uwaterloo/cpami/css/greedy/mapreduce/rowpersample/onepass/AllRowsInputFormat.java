package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample.onepass;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.mahout.math.MatrixWritable;

public class AllRowsInputFormat extends
		FileInputFormat<NullWritable, MatrixWritable> {

	@Override
	public RecordReader<NullWritable, MatrixWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new AllRowsRecordReaders(context.getConfiguration(),
				(FileSplit) split);
	}

}
