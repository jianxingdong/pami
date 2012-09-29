package ca.uwaterloo.cpami.mahout.matrix.utils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class Helpers {

	public static Matrix loadMatrix(Path fPath, int m, int n)
			throws IOException {
		Matrix mat = new SparseMatrix(m, n);
		final Configuration conf = new Configuration();
		final FileSystem fs = FileSystem.get(conf);
		FileStatus[] parts = fs.listStatus(fPath);

		IntWritable key = new IntWritable();
		VectorWritable vec = new VectorWritable();
		for (FileStatus part : parts) {
			Path p = part.getPath();
			if (!part.isDir() && !p.getName().startsWith("_")) {
				final SequenceFile.Reader reader = new SequenceFile.Reader(fs,
						p, conf);
				while (reader.next(key, vec)) {
					int i = key.get();
					Vector row = vec.get();
					Iterator<Vector.Element> itr = row.iterator();
					int j = 0;
					while (itr.hasNext()) {
						mat.set(i, j++, itr.next().get());
					}
				}
			}
		}
		return mat;
	}

	public static void writeMatrix(Matrix mat, Path fPath) throws IOException {
		int m = mat.numRows();
		int n = mat.numCols();
		final Configuration conf = new Configuration();
		final FileSystem fs = FileSystem.get(conf);
		final SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
				fPath, IntWritable.class, VectorWritable.class,
				CompressionType.BLOCK);

		double val = 0;
		for (int i = 0; i < m; i++) {
			RandomAccessSparseVector v = new RandomAccessSparseVector(n);
			for (int j = 0; j < n; j++) {
				if ((val = mat.get(i, j)) != 0) {
					v.set(j, val);
				}
			}
			writer.append(new IntWritable(i), new VectorWritable(v));
		}
		writer.close();
	}

	public static Path getDataFilePath(String outputDirectory)
			throws IOException {

		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] files = fs.listStatus(new Path(outputDirectory));
		for (FileStatus status : files) {
			Path p = status.getPath();
			if (!p.getName().startsWith("_"))
				return p;
		}
		return null;
	}

	public static void repartitionMatrix(Path input, Path output,
			int numPartitions) throws IOException, InterruptedException,
			ClassNotFoundException {
		final Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Helpers.class);
		FileInputFormat.addInputPaths(job, input.toString());
		FileOutputFormat.setOutputPath(job, output);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(numPartitions);
		job.waitForCompletion(false);
		if (!job.isSuccessful())
			throw new RuntimeException("Repartition Job is unsuccessful");

	}

	public static int getNumPrtitions(Path matrixPath) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		if (fs.getFileStatus(matrixPath).isDir()) {
			FileStatus[] files = fs.listStatus(matrixPath);
			int count = 0;
			for (FileStatus status : files) {
				Path p = status.getPath();
				if (!p.getName().startsWith("_"))
					count++;
			}
			return count;
		} else {
			return 1;
		}
	}
}