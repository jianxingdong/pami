package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class SelectedColumn implements Writable {

	private IntWritable columnIndex;
	private DoubleArrayWritable column;

	public SelectedColumn() {

	}

	public SelectedColumn(IntWritable columnIndex, DoubleArrayWritable column) {
		this.columnIndex = columnIndex;
		this.column = column;
	}

	public IntWritable getColumnIndex() {
		return columnIndex;
	}

	public ArrayWritable getColumn() {
		return column;
	}

	@Override
	public void readFields(DataInput dis) throws IOException {
		columnIndex = new IntWritable();
		columnIndex.readFields(dis);
		column = new DoubleArrayWritable();
		column.readFields(dis);
	}

	@Override
	public void write(DataOutput dos) throws IOException {
		columnIndex.write(dos);
		column.write(dos);
	}

}
