package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable {

	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}

	public DoubleArrayWritable(DoubleWritable[] colArr) {
		super(DoubleWritable.class, colArr);
	}

}
