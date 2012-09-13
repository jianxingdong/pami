package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.VectorWritable;

public class SelectedColumn implements Writable {

	private IntWritable columnIndex;
	private VectorWritable column;

	public SelectedColumn() {

	}

	public SelectedColumn(IntWritable columnIndex, VectorWritable column) {
		this.columnIndex = columnIndex;
		this.column = column;
	}

	public IntWritable getColumnIndex() {
		return columnIndex;
	}

	public VectorWritable getColumn() {
		return column;
	}

	@Override
	public void readFields(DataInput dis) throws IOException {
		columnIndex = new IntWritable();
		columnIndex.readFields(dis);
		column = new VectorWritable();
		column.readFields(dis);
	}

	@Override
	public void write(DataOutput dos) throws IOException {
		columnIndex.write(dos);
		column.write(dos);
	}

}
