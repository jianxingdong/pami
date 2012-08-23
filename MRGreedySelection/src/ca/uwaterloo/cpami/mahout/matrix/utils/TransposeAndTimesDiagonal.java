package ca.uwaterloo.cpami.mahout.matrix.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import ca.uwaterloo.cpami.css.dataprep.CSVToSequenceFile;
import ca.uwaterloo.cpami.css.dataprep.SequenceFileToCSV;

/**
 * 
 * A MapReduce Job to Transpose a matrix A and multiply it from the left to a
 * diagonal matrix S outputting a matrix X = S*A'.
 * 
 */
public class TransposeAndTimesDiagonal {

	public static class ElementWritable implements Writable {
		double value;
		int index;

		public ElementWritable(double value, int index) {
			this.value = value;
			this.index = index;
		}

		@Override
		public void readFields(DataInput dis) throws IOException {
			value = dis.readDouble();
			index = dis.readInt();
		}

		@Override
		public void write(DataOutput dos) throws IOException {
			dos.writeDouble(value);
			dos.writeInt(index);
		}
	}

	public class TTDMapper extends
			Mapper<IntWritable, VectorWritable, IntWritable, ElementWritable> {

		private Vector diagonal;

		protected void setup(
				org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, ElementWritable>.Context context)
				throws IOException, InterruptedException {
			// reading the diagonal from HDFS
			final Configuration conf = new Configuration();
			final FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path(context.getConfiguration()
					.get("diagonalFilePath")));
			diagonal = new DenseVector(context.getConfiguration().getInt(
					"diagonalLength", 0));
			try {
				int i = 0;
				while (true) {
					diagonal.set(i++, in.readDouble());
				}
			} catch (EOFException eof) {
				in.close();
			}

		};

		protected void map(
				IntWritable rowId,
				VectorWritable value,
				org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, ElementWritable>.Context context)
				throws java.io.IOException, InterruptedException {

			Vector row = value.get();
			Iterator<Vector.Element> itr = row.iterator();

			while (itr.hasNext()) {
				Vector.Element element = itr.next();
				double val = element.get() * diagonal.getQuick(element.index());
				// sending only the nonzero elements
				if (val > 1e-10 || val < -1e-10)
					context.write(new IntWritable(element.index()),
							new ElementWritable(val, rowId.get()));
			}

		};
	}

	public static class TTDReducer extends
			Reducer<IntWritable, ElementWritable, IntWritable, VectorWritable> {
		private int cardinality;

		protected void setup(
				org.apache.hadoop.mapreduce.Reducer<IntWritable, ElementWritable, IntWritable, VectorWritable>.Context context)
				throws IOException, InterruptedException {
			cardinality = context.getConfiguration().getInt("cardinality", 0);
		};

		protected void reduce(
				IntWritable colIndex,
				java.lang.Iterable<ElementWritable> col,
				org.apache.hadoop.mapreduce.Reducer<IntWritable, ElementWritable, IntWritable, VectorWritable>.Context context)
				throws java.io.IOException, InterruptedException {

			Vector colVector = new SequentialAccessSparseVector(cardinality);
			Iterator<ElementWritable> itr = col.iterator();
			while (itr.hasNext()) {
				ElementWritable element = itr.next();
				colVector.set(element.index, element.value);
			}

			context.write(colIndex, new VectorWritable(colVector));
		};
	}

	// test
	public static void main_(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		CSVToSequenceFile.csvToSequenceFile("/inv/inv-test.txt", ",", 100,
				"/inv/U");
		Vector diag = new SequentialAccessSparseVector(100);
		for (int i = 0; i < 100; i += 10)
			diag.set(i, 1);
		diag.set(1, 2);

		final Configuration conf = new Configuration();
		final FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path("/inv/D"));
		Iterator<Vector.Element> itr = diag.iterator();
		while (itr.hasNext())
			out.writeDouble(itr.next().get());
		out.close();

		Configuration config = new Configuration();
		config.setInt("cardinality", 100);
		config.setInt("diagonalLength", 100);
		config.setStrings("diagonalFilePath", "/inv/D");

		Job job = new Job(config);
		job.setJarByClass(TransposeAndTimesDiagonal.class);
		FileInputFormat.addInputPaths(job, "/inv/U");
		FileOutputFormat.setOutputPath(job, new Path("/inv/R"));
		job.setMapperClass(TTDMapper.class);
		job.setReducerClass(TTDReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setMapOutputValueClass(ElementWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		System.out.println("DONE--> Converting To CSV");
		SequenceFileToCSV.sequenceFileToCSV("/inv/R", "inv/R.csv", ",");

	}
}
