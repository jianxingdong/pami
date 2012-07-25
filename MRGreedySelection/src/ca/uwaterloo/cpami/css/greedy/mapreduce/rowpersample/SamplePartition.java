package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class SamplePartition implements Writable {

	private boolean isIndices;
	private ArrayWritable samplePart;
	private ArrayWritable colIndices;

	public boolean isIndices() {
		return isIndices;
	}

	public void setIndices(boolean isIndices) {
		this.isIndices = isIndices;

	}

	public Writable[] getSamplePart() {
		return samplePart.get();
	}

	public void setSamplePart(DoubleWritable[] samplePart) {
		this.samplePart = new ArrayWritable(DoubleWritable.class, samplePart);
	}

	public Writable[] getColIndices() {
		return colIndices.get();
	}

	public void setColIndices(IntWritable[] colIndices) {
		this.colIndices = new ArrayWritable(IntWritable.class, colIndices);
	}

	@Override
	public void readFields(DataInput dis) throws IOException {
		isIndices = dis.readBoolean();
		if (isIndices) {
			colIndices.readFields(dis);
		} else {
			samplePart.readFields(dis);
		}

	}

	@Override
	public void write(DataOutput dos) throws IOException {
		dos.writeBoolean(isIndices);
		if (isIndices)
			colIndices.write(dos);
		else
			samplePart.write(dos);
	}

}
